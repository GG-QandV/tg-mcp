import asyncio
import pytest
from pathlib import Path


@pytest.mark.asyncio
async def test_stale_socket_removed_on_start(tmp_sock_path):
    from src.mcp_telegram.daemon import _check_stale_socket

    Path(tmp_sock_path).touch()
    assert Path(tmp_sock_path).exists()

    await _check_stale_socket(tmp_sock_path)

    assert not Path(tmp_sock_path).exists()


@pytest.mark.asyncio
async def test_live_socket_causes_exit(tmp_sock_path):
    from src.mcp_telegram.daemon import _check_stale_socket

    async def handler(r, w):
        pass
    server = await asyncio.start_unix_server(handler, path=tmp_sock_path)

    with pytest.raises(SystemExit):
        await _check_stale_socket(tmp_sock_path)

    server.close()


@pytest.mark.asyncio
async def test_no_socket_no_error(tmp_sock_path):
    from src.mcp_telegram.daemon import _check_stale_socket
    await _check_stale_socket(tmp_sock_path)
