#!/bin/bash
# Run BROK MCP Server

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Activate venv and run server
source .venv/bin/activate
exec python brok_mcp_server.py
