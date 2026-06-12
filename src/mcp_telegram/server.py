from __future__ import annotations

import asyncio
import inspect
import logging
import typing as t
from collections.abc import Sequence
from functools import cache
import json
import traceback
from datetime import datetime
from pathlib import Path

from mcp.server import Server
from mcp.types import (
    EmbeddedResource,
    ImageContent,
    Prompt,
    Resource,
    ResourceTemplate,
    TextContent,
    Tool,
)

from . import tools
from . import inbox

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Debug logging для Perplexity
DEBUG_LOG_PATH = Path.home() / "Desktop" / "perplexity_mcp_debug.log"

def debug_log(message: str, data: t.Any = None) -> None:
    """Логирование отладочной информации для Perplexity."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    with open(DEBUG_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(f"\n[{timestamp}] {message}\n")
        if data is not None:
            f.write(f"Data type: {type(data).__name__}\n")
            try:
                f.write(f"Data: {json.dumps(data, indent=2, ensure_ascii=False)}\n")
            except:
                f.write(f"Data (repr): {repr(data)}\n")
        f.write("-" * 80 + "\n")
app = Server("mcp-telegram")


@cache
def enumerate_available_tools() -> t.Generator[tuple[str, Tool], t.Any, None]:
    for _, tool_args in inspect.getmembers(tools, inspect.isclass):
        if issubclass(tool_args, tools.ToolArgs) and tool_args != tools.ToolArgs:
            logger.debug("Found tool: %s", tool_args)
            description = tools.tool_description(tool_args)
            yield description.name, description


mapping: dict[str, Tool] = dict(enumerate_available_tools())


@app.list_prompts()
async def list_prompts() -> list[Prompt]:
    """List available prompts."""
    return []


@app.list_resources()
async def list_resources() -> list[Resource]:
    """List available resources."""
    return []


@app.list_tools()
async def list_tools() -> list[Tool]:
    """List available tools."""
    debug_log("list_tools called")
    tools_list = list(mapping.values())
    debug_log(f"Returning {len(tools_list)} tools", [t.model_dump() for t in tools_list][:3])  # Log first 3 tools
    return tools_list


@app.list_resource_templates()
async def list_resource_templates() -> list[ResourceTemplate]:
    """List available resource templates."""
    return []


@app.progress_notification()
async def progress_notification(pogress: str | int, p: float, s: float | None) -> None:
    """Progress notification."""


@app.call_tool()
async def call_tool(name: str, arguments: t.Any) -> Sequence[TextContent | ImageContent | EmbeddedResource]:  # noqa: ANN401
    """Handle tool calls for command line run."""
    
    # Детальное логирование для Perplexity
    debug_log(f"=== TOOL CALL: {name} ===")
    debug_log("Raw arguments received", arguments)
    debug_log(f"Arguments type: {type(arguments).__name__}")
    
    # Сохраняем в отдельный файл для быстрого доступа
    with open(Path.home() / "Desktop" / "last_mcp_call.json", "w") as f:
        import json
        f.write(json.dumps({
            "timestamp": datetime.now().isoformat(),
            "tool": name,
            "arguments": arguments if isinstance(arguments, dict) else str(arguments),
            "type": type(arguments).__name__
        }, indent=2))
    
    if arguments is None:
        debug_log("WARNING: arguments is None!")
        arguments = {}
    elif not isinstance(arguments, dict):
        debug_log(f"ERROR: arguments is not dict, it's {type(arguments).__name__}")
        raise TypeError(f"arguments must be dictionary, got {type(arguments).__name__}")

    debug_log("Arguments after processing", arguments)
    
    tool = mapping.get(name)
    if not tool:
        debug_log(f"ERROR: Unknown tool: {name}")
        debug_log("Available tools", list(mapping.keys()))
        raise ValueError(f"Unknown tool: {name}")

    debug_log(f"Found tool: {name}", tool.model_dump())
    
    try:
        debug_log("Creating tool args...")
        args = tools.tool_args(tool, **arguments)
        debug_log("Tool args created successfully", vars(args) if hasattr(args, '__dict__') else str(args))
        
        debug_log("Running tool...")
        result = await tools.tool_runner(args)
        debug_log("Tool completed successfully")
        debug_log(f"Result type: {type(result)}, Length: {len(result) if hasattr(result, '__len__') else 'N/A'}")
        
        # Детальное логирование результата
        if result:
            if isinstance(result, list):
                for i, item in enumerate(result[:3]):  # Log first 3 items
                    if hasattr(item, 'model_dump'):
                        debug_log(f"Result[{i}]", item.model_dump())
                    elif hasattr(item, '__dict__'):
                        debug_log(f"Result[{i}]", vars(item))
                    else:
                        debug_log(f"Result[{i}] type: {type(item).__name__}", str(item)[:500])
        
        return result
    except asyncio.CancelledError:
        debug_log("Tool execution was cancelled")
        raise
    except Exception as e:
        debug_log(f"ERROR in tool execution: {type(e).__name__}: {str(e)}")
        import traceback
        tb = traceback.format_exc()
        debug_log(f"Traceback:\n{tb}")
        
        # Сохраняем ошибку в отдельный файл
        with open(Path.home() / "Desktop" / "last_mcp_error.txt", "w") as f:
            f.write(f"Tool: {name}\n")
            f.write(f"Arguments: {arguments}\n")
            f.write(f"Error: {type(e).__name__}: {str(e)}\n")
            f.write(f"Traceback:\n{tb}\n")
        
        logger.exception("Error running tool: %s", name)
        
        # Возвращаем ошибку в читаемом виде вместо исключения
        return [TextContent(type="text", text=f"Error: {str(e)}")]


async def run_mcp_server() -> None:
    # Import here to avoid issues with event loops
    from mcp.server.stdio import stdio_server
    
    debug_log("=== MCP SERVER STARTING ===")
    debug_log(f"Server name: {app.name}")
    debug_log(f"Available tools: {len(mapping)}")
    debug_log("Tool names", list(mapping.keys()))

    # Start inbox listener (persistent Telegram connection for push)
    inbox_task = asyncio.create_task(inbox.start_listener())
    debug_log("Inbox listener task created")

    async with stdio_server() as (read_stream, write_stream):
        debug_log("stdio_server initialized, starting main loop...")
        try:
            await app.run(read_stream, write_stream, app.create_initialization_options())
        finally:
            debug_log("MCP server shutting down, stopping inbox listener...")
            await inbox.stop_listener()
            inbox_task.cancel()
            try:
                await inbox_task
            except asyncio.CancelledError:
                pass


def main() -> None:
    # Log startup to see if Perplexity is using this file
    import sys
    debug_log(f"=== STARTING MCP SERVER FROM: {__file__} ===")
    debug_log(f"Python executable: {sys.executable}")
    debug_log(f"Python path: {sys.path}")
    asyncio.run(run_mcp_server())
