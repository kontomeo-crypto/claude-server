# BROK MCP Server

Model Context Protocol server exposing BROK AI Platform services to Claude Code.

**Version:** 10.4
**Last Updated:** 2026-01-21

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  ADMIN TERMINALS (Claude → BROK)                                │
│  Claude Code → MCP Server → NATS/HTTP → BROK Services           │
│                                                                 │
│  HEADLESS BRIDGE (BROK → Claude)                                │
│  BROK → NATS → brok_claude_persistent.py → Claude CLI → MCP     │
└─────────────────────────────────────────────────────────────────┘
```

## Tools Available

### LENS (Vector Search)
| Tool | Description |
|------|-------------|
| `lens_search` | Search LENS vector database (14M+ vectors) with MUTE-based focus |
| `lens_context` | Get focused context optimized for LLM consumption |
| `lens_write` | Write data to LENS (solutions, knowledge, changes) |

### LLM Backends
| Tool | Description |
|------|-------------|
| `llm_query` | Query BROK LLM backends with auto-routing |
| `direct_gpu_swarm` | Direct access to GPU Swarm (8x Qwen2.5-1.5B) |
| `direct_qwen3_coder` | Direct access to Qwen3-Coder-30B (PRIMARY BRAIN) |
| `direct_qwen3_query` | Direct access to Qwen3-8B (query expansion) |
| `direct_vision` | Direct access to Vision-7B (image analysis) |
| `direct_math` | Direct access to Math-7B (calculations) |
| `direct_embed` | Direct embedding generation (768 dim) |

### BROK Core
| Tool | Description |
|------|-------------|
| `brok_chat` | Full pipeline: symbolic reasoning → LENS → LLM → curation |
| `brok_status` | System status - GPU utilization, service health |
| `brok_perf_monitor` | Performance metrics - latency, throughput, queues |
| `brok_service_control` | Service management - status, restart, logs |
| `brok_config_read` | Read configuration files |
| `brok_config_write` | Modify configuration (with backup) |

### Claude Bridge
| Tool | Description |
|------|-------------|
| `get_pending_task` | Get next task from BROK for Claude to process |
| `submit_solution` | Submit solution for a task |
| `send_heartbeat` | Health heartbeat (call every 500ms) |
| `log_code_change` | Log code changes for audit trail |

### CNC Tools
| Tool | Description |
|------|-------------|
| `cnc_generate_gcode` | Generate G-code from description |
| `cnc_validate_gcode` | Validate G-code for safety/correctness |
| `cnc_optimize_gcode` | Optimize for speed/finish/tool life |
| `cnc_analyze_part` | Analyze part for manufacturability |
| `cnc_calculate_feeds_speeds` | Calculate optimal feeds and speeds |

### Code Tools
| Tool | Description |
|------|-------------|
| `code_generate` | Generate code with LENS context |
| `code_review` | Review for bugs, security, performance |
| `code_explain` | Explain code in plain language |
| `code_refactor` | Refactor for readability/performance |
| `code_complete` | Complete partial code |
| `code_fix` | Fix code given error message |

### Advanced
| Tool | Description |
|------|-------------|
| `brok_nats_publish` | Publish to any NATS topic |
| `brok_nats_request` | Request/reply to NATS services |
| `brok_output_correct` | Correct/override BROK outputs |
| `brok_workflow_modify` | Control active workflows |
| `brok_escalation_override` | Override LLM tier routing |

## Installation

### Claude Code Configuration

Add to `~/.claude.json` under `mcpServers`:

```json
{
  "mcpServers": {
    "brok": {
      "type": "stdio",
      "command": "/home/kontomeo/brok-mcp-server/.venv/bin/python",
      "args": [
        "/home/kontomeo/brok-mcp-server/brok_mcp_server.py",
        "--user-id=user_1"
      ],
      "env": {
        "BROK_NATS_URL": "nats://127.0.0.1:42222",
        "BROK_HTTP_URL": "http://127.0.0.1:12222",
        "BROK_USER_ID": "user_1",
        "BROK_NATS_USER": "brok_service",
        "BROK_NATS_PASSWORD": "Br0k_N4ts_S3cur3_2026!"
      }
    }
  }
}
```

**IMPORTANT:** Do NOT escape the `!` in the password. Use `Br0k_N4ts_S3cur3_2026!` not `Br0k_N4ts_S3cur3_2026\!`.

### Dependencies

```bash
cd /home/kontomeo/brok-mcp-server
python -m venv .venv
source .venv/bin/activate
pip install mcp nats-py aiohttp orjson
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `BROK_NATS_URL` | `nats://127.0.0.1:42222` | NATS server URL |
| `BROK_HTTP_URL` | `http://127.0.0.1:12222` | BROK Core HTTP API |
| `BROK_USER_ID` | `user_1` | User ID for isolation |
| `BROK_NATS_USER` | `brok_service` | NATS auth username |
| `BROK_NATS_PASSWORD` | (required) | NATS auth password |

## Performance

| Backend | Typical Latency |
|---------|-----------------|
| GPU Swarm | 50-70ms |
| Qwen3-Coder-30B | 400-500ms |
| Vision-7B | 180-200ms |
| Embedding | 15-20ms |
| LENS Search | 2-3s (14M vectors) |

## Troubleshooting

### MCP returns empty LENS results

**Symptom:** `lens_search` returns `{"status": "success", "results": [], "count": 0}`

**Cause:** NATS authentication failure, falling back to HTTP which returns 404

**Fix:**
1. Check NATS password in `~/.claude.json` - ensure no backslash before `!`
2. Restart Claude Code after config changes
3. Verify NATS auth: `python -c "import asyncio, nats; asyncio.run(nats.connect('nats://brok_service:PASSWORD@127.0.0.1:42222'))"`

### MCP server not connected

**Symptom:** `MCP server brok is not connected`

**Fix:**
1. Restart Claude Code
2. Check MCP server can start: `python brok_mcp_server.py --user-id=user_1`
3. Check for Python errors in the server

### NATS timeout

**Symptom:** Tool calls timeout after 30 seconds

**Fix:**
1. Verify NATS is running: `curl http://127.0.0.1:52222/varz`
2. Check BROK services: `systemctl status brok-*`
3. Check LENS service: `ps aux | grep brok_lens`

## Changelog

### v10.4 (2026-01-21)
- Fixed NATS auth issue causing empty LENS results
- Added fresh connection per request (MCP event loop workaround)
- Fixed HTTP fallback treating 404 as success
- Added debug logging to trace NATS connectivity
- Added startup NATS connectivity test

### v10.3 (2026-01-20)
- Added Claude bridge tools (get_pending_task, submit_solution)
- Added CNC tools (generate, validate, optimize)
- Added code tools (generate, review, explain, refactor)
- User isolation via path-based ownership

### v10.0 (2026-01-17)
- Initial release with LENS, LLM, and BROK Core tools
- NATS authentication support
- HTTP fallback for resilience

## License

Proprietary - KONTOMEO LLC
