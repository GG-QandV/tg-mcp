# Architecture v2: tg-mcpd + IPC Proxy

## Context

5+ AI agents в разных Telegram топиках, одна машина. stdio MCP не масштабируется 1:1 — нужен daemon с IPC.

## Containers (L2)

```
┌──────────────────────────────────────────────────────┐
│                    tg-mcpd daemon                     │
│                                                      │
│  ┌────────────────────────────────────────────────┐  │
│  │           One TelegramClient                    │  │
│  │           (persistent MTProto connection)       │  │
│  └───────┬────────────────────────────────┬────────┘  │
│          │                                │           │
│  ┌───────┴────────┐             ┌─────────┴────────┐  │
│  │   Inbox Engine  │             │   IPC Server      │  │
│  │                 │             │   (Unix socket)    │  │
│  │  topic_map[     │             │                    │  │
│  │    topic_id →   │             │  ┌──────────────┐  │  │
│  │      buffer[]   │             │  │  /tmp/tgmcpd  │  │  │
│  │  ]              │             │  └──────────────┘  │  │
│  └─────────────────┘             └─────────┬──────────┘  │
└────────────────────────────────────────────┼──────────────┘
                                             │
         ┌────────────────┬──────────────────┼──────────────────┐
         │                │                  │                  │
  ┌──────┴──────┐  ┌──────┴──────┐  ┌──────┴──────┐  ┌──────┴──────┐
  │ tg-mcp      │  │ tg-mcp      │  │ tg-mcp      │  │ tg-mcp      │
  │ proxy       │  │ proxy       │  │ proxy       │  │ proxy       │
  │ topic=205   │  │ topic=310   │  │ topic=415   │  │ topic=...   │
  │             │  │             │  │             │  │             │
  │ MCP stdio   │  │ MCP stdio   │  │ MCP stdio   │  │ MCP stdio   │
  │ ↔ IPC       │  │ ↔ IPC       │  │ ↔ IPC       │  │ ↔ IPC       │
  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘
         │                 │                 │                │
    ┌────┴────┐       ┌────┴────┐       ┌────┴────┐      ┌────┴────┐
    │Agent 1  │       │Agent 2  │       │Agent 3  │      │Agent 4  │
    │opencode │       │opencode │       │opencode │      │opencode │
    └─────────┘       └─────────┘       └─────────┘      └─────────┘
```

## IPC Protocol

JSON-line поверх Unix socket:

```
→ REQ {"method":"send_message","params":{"chat_id":...,"text":"..."},"id":1}
← RES {"result":{...},"id":1}

→ REQ {"method":"inbox_poll","params":{"topic_id":205},"id":2}
← RES {"result":[{"id":...,"text":"..."}],"id":2}
```

Без MCP поверху — daemon принимает сырые вызовы, proxy переводит MCP JSON-RPC в IPC протокол.

## Components

### tg-mcpd (daemon)

| Aspect | Detail |
|--------|--------|
| Язык | Python |
| Запуск | systemd-сервис, `Restart=always` |
| Сеть | Unix socket `/tmp/tgmcpd.sock` |
| Telegram | Один TelethonClient, persistent |
| Inbox | Один event handler, кладёт в буфер по `topic_id` |
| Auth | Никакой — Unix socket, проверка `PID` через `SO_PEERCRED` |

### tg-mcp proxy

| Aspect | Detail |
|--------|--------|
| Язык | Python (~30 строк) |
| Запуск | opencode через `run_proxy_205.sh` |
| Lifecycle | Читает MCP stdio → шлёт IPC → возвращает результат |
| Состояние | Нет. Полностью stateless. |
| Топик | Фиксирован через env `TG_TOPIC_ID` |

## Data Flow

```
SendMessage (агент → Telegram):

  Agent → [stdio] → proxy → [IPC Unix socket] → tg-mcpd → [MTProto] → Telegram
  
Inbox (Telegram → агент):

  Telegram → [MTProto event] → tg-mcpd → buffer[topic_id]
    
  Agent → [stdio] → InboxRead → proxy → [IPC] → tg-mcpd → drain buffer[topic_id]
```

## Lifecycle

```
systemctl start tg-mcpd
  ├── TelegramClient()
  ├── client.start()
  ├── Inbox engine loop
  ├── IPC server on /tmp/tgmcpd.sock
  └── Блокируется на asyncio.get_event_loop().run_forever()

opencode → run_proxy_205.sh → tg-mcp proxy
  ├── connect to /tmp/tgmcpd.sock
  ├── MCP initialize → list_tools
  └── loop: read stdio → IPC → write stdio
```

## Reliability

| Точка отказа | Последствия | Восстановление |
|---|---|---|
| tg-mcpd упал | Все proxy теряют соединение | systemd restart → proxy auto-reconnect |
| proxy упал | Один агент теряет MCP | opencode restart → proxy |
| Telegram disconnect | Все тулзы не работают | Telethon auto-reconnect |
| Session файл битый | daemon не стартует | Alert: перелогиниться |

## Migration Path from v1

1. Написать tg-mcpd (выделить из server.py inbox + добавить IPC)
2. Написать proxy (тонкий stdio ↔ IPC мост)
3. Добавить systemd unit
4. Заменить `run_server.sh` на `run_proxy.sh`
5. Оттестировать с одним агентом
6. Добавить второго агента с другим топиком

## Workflow

### Шаг 1: tg-mcpd

```
src/mcp_telegram/
  daemon.py         # main: TelegramClient + IPC Server
  inbox.py          # (v1) без изменений, буфер
  ipc_server.py     # Unix socket, JSON-line протокол
  telegram.py       # (v1) TelegramSettings
```

- `daemon.py` создаёт TelegramClient, регистрирует inbox handler, запускает IPC server
- `build_daemon.sh` — установка как systemd сервис

### Шаг 2: proxy

```
src/mcp_telegram/
  proxy.py          # MCP stdio сервер → IPC клиент
```

- Регистрирует те же 50 tools, но каждый вызов пересылает через IPC
- InboxRead/InboxPeek шлют `topic_id` из `TG_TOPIC_ID`

### Шаг 3: systemd

```
/etc/systemd/system/tgmcpd.service
  [Unit]
  Description=Telegram MCP daemon
  
  [Service]
  ExecStart=/usr/local/bin/tgmcpd
  Restart=always
  RestartSec=5
```

### Шаг 4: proxy launcher

```
run_proxy_205.sh:
  export TG_TOPIC_ID=205
  while true; do tg-mcp-proxy; sleep 1; done

opencode.json:
  "tg-topic-205": {
    "command": "bash",
    "args": ["run_proxy_205.sh"]
  }
```

### Шаг 5: repeat

Скопировать `run_proxy_205.sh` → `run_proxy_310.sh` с другим `TG_TOPIC_ID`.
Каждый opencode сессион подключает свой proxy.

## Files

| File | Purpose |
|---|---|
| `src/mcp_telegram/daemon.py` | Daemon entry point |
| `src/mcp_telegram/ipc_server.py` | Unix socket JSON-line server |
| `src/mcp_telegram/proxy.py` | stdio ↔ IPC bridge |
| `scripts/tgmcpd.service` | systemd unit |
| `scripts/tgmcpd-build.sh` | Install script |
| `scripts/run_proxy.sh` | Proxy launcher (param: topic) |
| `docs/architecture-v2.md` | This file |
