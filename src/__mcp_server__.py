"""Run IBPython MCP server.

Usage:
    python -m src.mcp_server              # stdio (for Claude Desktop, Cursor)
    python -m src.mcp_server --http       # Streamable HTTP on port 9000
"""

import sys


def main() -> None:
    from src.mcp_server import main_stdio, main_streamable_http

    if "--http" in sys.argv:
        main_streamable_http()
    else:
        main_stdio()


if __name__ == "__main__":
    main()
