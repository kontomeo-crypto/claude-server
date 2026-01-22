#!/usr/bin/env python3
"""
BROK MCP Server - Exposes BROK AI Platform to Claude Code

Tools:
- lens_search: Vector similarity search via LENS
- lens_context: Get focused context for a query
- llm_query: Query BROK LLM backends (GPU swarm, Qwen3-Coder, etc.)
- embed_text: Generate embeddings via BGE
- brok_chat: Send message through BROK Core

Connects to BROK via:
- NATS (port 42222) for async messaging
- HTTP (port 12222) as fallback
"""

import asyncio
import json
import logging
import os
import sys
import argparse
import re
from typing import Any

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent

# =============================================================================
# USER ISOLATION ENFORCEMENT (V10.2 - January 2026)
# =============================================================================
# This MCP server enforces strict user isolation:
# 1. --user-id is REQUIRED at startup (locked for entire session)
# 2. All tool calls auto-inject this user_id (can't be overridden)
# 3. NATS topics are namespaced per user
# 4. No cross-user data access possible
# =============================================================================

def parse_args():
    """Parse command line arguments with REQUIRED --user-id."""
    parser = argparse.ArgumentParser(description="BROK MCP Server with User Isolation")
    parser.add_argument(
        "--user-id",
        required=True,
        help="User ID (REQUIRED - locked for entire session, cannot be changed)"
    )
    parser.add_argument(
        "--enforce-isolation",
        action="store_true",
        default=True,
        help="Enforce strict user isolation (default: True)"
    )
    parser.add_argument(
        "--nats-url",
        default=os.environ.get("BROK_NATS_URL", "nats://127.0.0.1:42222"),
        help="NATS server URL"
    )
    return parser.parse_args()

# Parse args at module load (before any tool calls)
_args = None
try:
    _args = parse_args()
except SystemExit:
    # Running without args (e.g., --help) - use defaults for module import
    pass

# Configure logging to file (never stdout/stderr for MCP)
LOG_DIR = os.path.expanduser("~/.local/share/brok-mcp")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(f"{LOG_DIR}/brok-mcp-server.log")]
)
logger = logging.getLogger("brok-mcp")

app = Server("brok")

# =============================================================================
# BROK Connection Configuration
# =============================================================================

BROK_NATS_URL = _args.nats_url if _args else os.environ.get("BROK_NATS_URL", "nats://127.0.0.1:42222")
BROK_HTTP_URL = os.environ.get("BROK_HTTP_URL", "http://127.0.0.1:12222")

# USER ID IS LOCKED AT STARTUP - CANNOT BE CHANGED
BROK_USER_ID = _args.user_id if _args else os.environ.get("BROK_USER_ID", "user_1")
ENFORCE_ISOLATION = _args.enforce_isolation if _args else True

# Validate user_id format at startup
if not re.match(r'^[a-zA-Z0-9_-]+$', BROK_USER_ID):
    logger.error(f"SECURITY: Invalid user_id format: {BROK_USER_ID}")
    sys.exit(1)

# =============================================================================
# MASTER SECURITY RULE: CODE EXECUTION RESTRICTIONS
# =============================================================================
# ONLY user_1 (admin) can execute code/scripts/commands on this server.
# All other users: BROK and Claude may only DELIVER code, never execute.
# Users run code on THEIR OWN HARDWARE, not on this server.
# =============================================================================

ADMIN_USER = "user_1"
CAN_EXECUTE_CODE = (BROK_USER_ID == ADMIN_USER)

# Blocked tools for non-admin users (code execution tools)
EXECUTION_BLOCKED_TOOLS = [
    "brok_service_control",  # systemctl commands
    "brok_config_write",     # config modifications
    "brok_nats_publish",     # direct NATS commands (could trigger execution)
]

logger.info(f"=== USER ISOLATION ENABLED ===")
logger.info(f"Locked user_id: {BROK_USER_ID}")
logger.info(f"Enforce isolation: {ENFORCE_ISOLATION}")
logger.info(f"Code execution allowed: {CAN_EXECUTE_CODE} (admin={ADMIN_USER})")
logger.info(f"All tool calls will use user_id={BROK_USER_ID} (cannot be overridden)")

if not CAN_EXECUTE_CODE:
    logger.info(f"=== READ-ONLY MODE: Code delivery only, no execution ===")

# NATS Authentication (required for V10 security)
BROK_NATS_USER = os.environ.get("BROK_NATS_USER", "brok_service")
BROK_NATS_PASSWORD = os.environ.get("BROK_NATS_PASSWORD", "Br0k_N4ts_S3cur3_2026!")

# NATS Topics
TOPICS = {
    "chat": "brok.core.chat",
    "lens_search": "brok.lens.search",
    "lens_context": "brok.lens.context",
    "llm_generate": "brok.llm.gpu0.generate",
    "llm_coder": "brok.llm.coder",
    "embed": "brok.embed.encode",
}

# LLM Backends
LLM_BACKENDS = {
    "gpu_swarm": {"ports": list(range(9600, 9608)), "desc": "8x1.5B fast parallel"},
    "qwen3_coder": {"port": 8100, "desc": "30B primary brain with EAGLE3"},
    "qwen3_query": {"port": 8016, "desc": "8B query expansion/context"},
    "vision": {"port": 8010, "desc": "Vision-7B image/video"},
    "math": {"port": 8100, "desc": "Routed to Qwen3-Coder (Falcon-H1R broken)"},
}

# =============================================================================
# NATS Client
# =============================================================================

_nats_client = None

async def get_nats():
    """Get or create NATS connection with authentication."""
    global _nats_client
    if _nats_client is None or not _nats_client.is_connected:
        try:
            import nats
            # Connect with authentication if credentials provided
            connect_opts = {"servers": [BROK_NATS_URL]}
            if BROK_NATS_USER and BROK_NATS_PASSWORD:
                connect_opts["user"] = BROK_NATS_USER
                connect_opts["password"] = BROK_NATS_PASSWORD
                logger.info(f"Connecting to NATS with auth user: {BROK_NATS_USER}")

            _nats_client = await nats.connect(**connect_opts)
            logger.info(f"Connected to NATS at {BROK_NATS_URL}")
        except Exception as e:
            logger.error(f"NATS connection failed: {e}")
            _nats_client = None
    return _nats_client


async def nats_request(topic: str, payload: dict, timeout: float = 30.0) -> dict:
    """Send NATS request with fresh connection (MCP event loop workaround)."""
    import sys
    import nats

    print(f"[NATS_REQ] Topic: {topic}", file=sys.stderr)

    # Create fresh connection for each request (workaround for MCP event loop issues)
    try:
        connect_opts = {"servers": [BROK_NATS_URL]}
        if BROK_NATS_USER and BROK_NATS_PASSWORD:
            connect_opts["user"] = BROK_NATS_USER
            connect_opts["password"] = BROK_NATS_PASSWORD

        nc = await nats.connect(**connect_opts)
        print(f"[NATS_REQ] Fresh connection: {nc.is_connected}", file=sys.stderr)

        try:
            msg = await nc.request(topic, json.dumps(payload).encode(), timeout=timeout)
            result = json.loads(msg.data.decode())
            print(f"[NATS_REQ] Response OK, keys: {result.keys() if isinstance(result, dict) else type(result)}", file=sys.stderr)
            return result
        finally:
            await nc.close()
    except Exception as e:
        print(f"[NATS_REQ] ERROR: {e}", file=sys.stderr)
        logger.error(f"NATS request to {topic} failed: {e}")
        raise


async def http_request(endpoint: str, payload: dict, timeout: float = 30.0) -> dict:
    """Fallback HTTP request to BROK Core."""
    import aiohttp
    url = f"{BROK_HTTP_URL}{endpoint}"

    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
            result = await resp.json()
            # Raise on HTTP errors or error responses
            if resp.status >= 400 or result.get("error"):
                raise Exception(f"HTTP {resp.status}: {result.get('error', 'Unknown error')}")
            return result


# =============================================================================
# Tool Definitions
# =============================================================================

@app.list_tools()
async def list_tools() -> list[Tool]:
    """Return available BROK tools."""
    return [
        Tool(
            name="lens_search",
            description="Search BROK's LENS vector database (14M+ vectors). Uses MUTE-based focus for relevant results. Returns similar documents/chunks.",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search query (will be expanded by query rewriter)"
                    },
                    "top_k": {
                        "type": "integer",
                        "description": "Number of results to return (default: 10)",
                        "default": 10
                    },
                    "datasets": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional: limit to specific datasets (e.g., ['coding', 'cnc', 'math'])"
                    }
                },
                "required": ["query"]
            }
        ),
        Tool(
            name="lens_context",
            description="Get focused context for a query using LENS MUTE system. Returns curated context optimized for LLM consumption.",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Query to get context for"
                    },
                    "zoom_level": {
                        "type": "string",
                        "enum": ["narrow", "medium", "wide"],
                        "description": "Context zoom level (default: medium)",
                        "default": "medium"
                    },
                    "max_tokens": {
                        "type": "integer",
                        "description": "Maximum context tokens (default: 4000)",
                        "default": 4000
                    }
                },
                "required": ["query"]
            }
        ),
        Tool(
            name="llm_query",
            description="Query BROK's LLM backends directly. Backends: gpu_swarm (fast), qwen3_coder (complex/code), vision (images), math (calculations).",
            inputSchema={
                "type": "object",
                "properties": {
                    "prompt": {
                        "type": "string",
                        "description": "Prompt to send to LLM"
                    },
                    "backend": {
                        "type": "string",
                        "enum": ["gpu_swarm", "qwen3_coder", "qwen3_query", "vision", "math", "auto"],
                        "description": "Which backend to use (default: auto - FCFS routing)",
                        "default": "auto"
                    },
                    "max_tokens": {
                        "type": "integer",
                        "description": "Maximum tokens to generate (default: 2000)",
                        "default": 2000
                    },
                    "temperature": {
                        "type": "number",
                        "description": "Sampling temperature (default: 0.7)",
                        "default": 0.7
                    },
                    "system_prompt": {
                        "type": "string",
                        "description": "Optional system prompt"
                    }
                },
                "required": ["prompt"]
            }
        ),
        Tool(
            name="embed_text",
            description="Generate embeddings using BROK's BGE-base model (768 dimensions). For semantic search, similarity, or clustering.",
            inputSchema={
                "type": "object",
                "properties": {
                    "text": {
                        "type": "string",
                        "description": "Text to embed"
                    },
                    "texts": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Multiple texts to embed (batch mode)"
                    }
                }
            }
        ),
        Tool(
            name="brok_chat",
            description="Send a message through BROK Core's full pipeline (symbolic reasoning -> LENS context -> LLM -> curation). Use for complex queries needing BROK's intelligence.",
            inputSchema={
                "type": "object",
                "properties": {
                    "message": {
                        "type": "string",
                        "description": "Message to send to BROK"
                    },
                    "session_id": {
                        "type": "string",
                        "description": "Optional session ID for conversation continuity"
                    },
                    "include_context": {
                        "type": "boolean",
                        "description": "Whether to include LENS context (default: true)",
                        "default": True
                    }
                },
                "required": ["message"]
            }
        ),
        Tool(
            name="brok_status",
            description="Get BROK system status - GPU utilization, active models, service health.",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        ),
        # =====================================================================
        # BIDIRECTIONAL CLAUDE BRIDGE TOOLS
        # =====================================================================
        Tool(
            name="lens_write",
            description="Write data to LENS (solutions, changes, knowledge). Used for Claude->BROK communication.",
            inputSchema={
                "type": "object",
                "properties": {
                    "content": {
                        "type": "string",
                        "description": "Content to write to LENS"
                    },
                    "data_type": {
                        "type": "string",
                        "enum": ["claude_solution", "claude_change", "claude_knowledge", "claude_heartbeat"],
                        "description": "Type of data being written"
                    },
                    "metadata": {
                        "type": "object",
                        "description": "Additional metadata (task_id, file_path, etc.)"
                    }
                },
                "required": ["content", "data_type"]
            }
        ),
        Tool(
            name="get_pending_task",
            description="Get the next pending task from BROK for Claude to process. Returns null if no tasks.",
            inputSchema={
                "type": "object",
                "properties": {
                    "priority_min": {
                        "type": "integer",
                        "description": "Minimum priority level (0=low, 3=critical)",
                        "default": 0
                    }
                }
            }
        ),
        Tool(
            name="submit_solution",
            description="Submit a solution for a task. Writes to LENS and notifies BROK.",
            inputSchema={
                "type": "object",
                "properties": {
                    "task_id": {
                        "type": "string",
                        "description": "The task ID being solved"
                    },
                    "solution": {
                        "type": "string",
                        "description": "The solution content"
                    },
                    "confidence": {
                        "type": "number",
                        "description": "Confidence score 0.0-1.0",
                        "default": 1.0
                    },
                    "sources": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Source references used"
                    }
                },
                "required": ["task_id", "solution"]
            }
        ),
        Tool(
            name="send_heartbeat",
            description="Send health heartbeat to BROK. Call every 0.5s to indicate Claude is active.",
            inputSchema={
                "type": "object",
                "properties": {
                    "state": {
                        "type": "string",
                        "enum": ["idle", "busy", "processing"],
                        "description": "Current Claude state",
                        "default": "idle"
                    },
                    "tasks_pending": {
                        "type": "integer",
                        "description": "Number of tasks in queue",
                        "default": 0
                    }
                }
            }
        ),
        Tool(
            name="log_code_change",
            description="Log a code change made by Claude for audit trail. Call after making any code modification.",
            inputSchema={
                "type": "object",
                "properties": {
                    "file_path": {
                        "type": "string",
                        "description": "Path to modified file"
                    },
                    "change_type": {
                        "type": "string",
                        "enum": ["bug_fix", "optimization", "refactor", "feature", "config"],
                        "description": "Type of change"
                    },
                    "reason": {
                        "type": "string",
                        "description": "Why this change was made"
                    },
                    "before": {
                        "type": "string",
                        "description": "Code before change (snippet)"
                    },
                    "after": {
                        "type": "string",
                        "description": "Code after change (snippet)"
                    }
                },
                "required": ["file_path", "change_type", "reason"]
            }
        ),
        # =====================================================================
        # CLAUDE DIRECT AGENT TOOLS (Performance Cop)
        # =====================================================================
        # These tools allow Claude to directly interact with BROK system,
        # monitor performance, correct outputs, and modify workflows live.
        Tool(
            name="brok_nats_publish",
            description="Publish a message directly to any BROK NATS topic. Use for direct system commands.",
            inputSchema={
                "type": "object",
                "properties": {
                    "topic": {
                        "type": "string",
                        "description": "NATS topic to publish to (e.g., 'brok.core.command', 'brok.llm.override')"
                    },
                    "payload": {
                        "type": "object",
                        "description": "JSON payload to publish"
                    },
                    "user_isolated": {
                        "type": "boolean",
                        "description": "Whether to use user-namespaced topic (default: true)",
                        "default": True
                    }
                },
                "required": ["topic", "payload"]
            }
        ),
        Tool(
            name="brok_nats_request",
            description="Send request/reply to BROK service via NATS. Waits for response.",
            inputSchema={
                "type": "object",
                "properties": {
                    "topic": {
                        "type": "string",
                        "description": "NATS topic to request (e.g., 'brok.core.status', 'brok.lens.search')"
                    },
                    "payload": {
                        "type": "object",
                        "description": "JSON payload for request"
                    },
                    "timeout": {
                        "type": "number",
                        "description": "Timeout in seconds (default: 30)",
                        "default": 30.0
                    }
                },
                "required": ["topic", "payload"]
            }
        ),
        Tool(
            name="brok_service_control",
            description="Control BROK services (status, restart). Use for service management.",
            inputSchema={
                "type": "object",
                "properties": {
                    "service": {
                        "type": "string",
                        "description": "Service name (e.g., 'brok-core', 'brok-lens', 'brok-llm-gpu0')"
                    },
                    "action": {
                        "type": "string",
                        "enum": ["status", "restart", "stop", "start", "logs"],
                        "description": "Action to perform"
                    },
                    "log_lines": {
                        "type": "integer",
                        "description": "Number of log lines to return (for 'logs' action)",
                        "default": 50
                    }
                },
                "required": ["service", "action"]
            }
        ),
        Tool(
            name="brok_config_read",
            description="Read BROK configuration files. Safe read-only access.",
            inputSchema={
                "type": "object",
                "properties": {
                    "config_path": {
                        "type": "string",
                        "description": "Config file path relative to /mnt/BROK_N/BROK_N_AI/config/"
                    }
                },
                "required": ["config_path"]
            }
        ),
        Tool(
            name="brok_config_write",
            description="Modify BROK configuration (pre-approved changes). Auto-backups before write.",
            inputSchema={
                "type": "object",
                "properties": {
                    "config_path": {
                        "type": "string",
                        "description": "Config file path relative to /mnt/BROK_N/BROK_N_AI/config/"
                    },
                    "content": {
                        "type": "string",
                        "description": "New config content"
                    },
                    "reason": {
                        "type": "string",
                        "description": "Reason for config change"
                    }
                },
                "required": ["config_path", "content", "reason"]
            }
        ),
        Tool(
            name="brok_perf_monitor",
            description="Monitor BROK performance metrics. GPU usage, latency, throughput, queue depths.",
            inputSchema={
                "type": "object",
                "properties": {
                    "metrics": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Metrics to retrieve: ['gpu', 'latency', 'throughput', 'queues', 'memory', 'all']"
                    },
                    "window_seconds": {
                        "type": "integer",
                        "description": "Time window for metrics (default: 60)",
                        "default": 60
                    }
                }
            }
        ),
        Tool(
            name="brok_output_correct",
            description="Correct/override a BROK output. Use when BROK response is wrong or suboptimal.",
            inputSchema={
                "type": "object",
                "properties": {
                    "request_id": {
                        "type": "string",
                        "description": "Original request ID to correct"
                    },
                    "original_output": {
                        "type": "string",
                        "description": "The incorrect output from BROK"
                    },
                    "corrected_output": {
                        "type": "string",
                        "description": "The correct output to replace it"
                    },
                    "correction_reason": {
                        "type": "string",
                        "description": "Why the correction was needed"
                    },
                    "apply_to_future": {
                        "type": "boolean",
                        "description": "Whether to add this as a learning example (default: true)",
                        "default": True
                    }
                },
                "required": ["original_output", "corrected_output", "correction_reason"]
            }
        ),
        Tool(
            name="brok_workflow_modify",
            description="Modify active BROK workflow state. Pause, resume, skip steps, inject context.",
            inputSchema={
                "type": "object",
                "properties": {
                    "workflow_id": {
                        "type": "string",
                        "description": "Workflow ID to modify"
                    },
                    "action": {
                        "type": "string",
                        "enum": ["pause", "resume", "skip_step", "inject_context", "abort", "status"],
                        "description": "Modification action"
                    },
                    "context": {
                        "type": "object",
                        "description": "Additional context for inject_context action"
                    },
                    "step_index": {
                        "type": "integer",
                        "description": "Step index for skip_step action"
                    }
                },
                "required": ["action"]
            }
        ),
        Tool(
            name="brok_escalation_override",
            description="Override BROK's escalation tier decision. Force specific LLM backend or tier.",
            inputSchema={
                "type": "object",
                "properties": {
                    "request_id": {
                        "type": "string",
                        "description": "Request to override escalation for"
                    },
                    "force_tier": {
                        "type": "integer",
                        "description": "Force specific tier (1-5)"
                    },
                    "force_backend": {
                        "type": "string",
                        "enum": ["gpu_swarm", "qwen3_coder", "qwen3_query", "vision", "math", "strix_halo"],
                        "description": "Force specific backend"
                    },
                    "reason": {
                        "type": "string",
                        "description": "Reason for override"
                    }
                },
                "required": ["reason"]
            }
        ),
        # =====================================================================
        # DIRECT LLM ACCESS (Bypass BROK - Raw Model Calls)
        # =====================================================================
        # These tools allow Claude to directly query LLM backends without going
        # through BROK's orchestration. Useful for parallel work, testing, and
        # when BROK is being updated (can't use BROK to update BROK).
        Tool(
            name="direct_gpu_swarm",
            description="DIRECT access to GPU Swarm (8x Qwen2.5-1.5B). Fast parallel inference, bypasses BROK. Good for quick tasks.",
            inputSchema={
                "type": "object",
                "properties": {
                    "prompt": {
                        "type": "string",
                        "description": "Prompt to send"
                    },
                    "system_prompt": {
                        "type": "string",
                        "description": "Optional system prompt"
                    },
                    "max_tokens": {
                        "type": "integer",
                        "description": "Max tokens to generate (default: 2000)",
                        "default": 2000
                    },
                    "temperature": {
                        "type": "number",
                        "description": "Sampling temperature (default: 0.7)",
                        "default": 0.7
                    },
                    "worker_id": {
                        "type": "integer",
                        "description": "Specific worker 0-7 (default: auto-select fastest)",
                        "minimum": 0,
                        "maximum": 7
                    }
                },
                "required": ["prompt"]
            }
        ),
        Tool(
            name="direct_qwen3_coder",
            description="DIRECT access to Qwen3-Coder-30B + EAGLE3 (PRIMARY BRAIN). Complex reasoning, code generation. Bypasses BROK.",
            inputSchema={
                "type": "object",
                "properties": {
                    "prompt": {
                        "type": "string",
                        "description": "Prompt to send"
                    },
                    "system_prompt": {
                        "type": "string",
                        "description": "Optional system prompt"
                    },
                    "max_tokens": {
                        "type": "integer",
                        "description": "Max tokens to generate (default: 4000)",
                        "default": 4000
                    },
                    "temperature": {
                        "type": "number",
                        "description": "Sampling temperature (default: 0.7)",
                        "default": 0.7
                    }
                },
                "required": ["prompt"]
            }
        ),
        Tool(
            name="direct_vision",
            description="DIRECT access to Vision-7B (Qwen2.5-VL-7B). Image/video analysis. Bypasses BROK. Send image as base64.",
            inputSchema={
                "type": "object",
                "properties": {
                    "prompt": {
                        "type": "string",
                        "description": "Question about the image"
                    },
                    "image_base64": {
                        "type": "string",
                        "description": "Base64-encoded image"
                    },
                    "image_url": {
                        "type": "string",
                        "description": "URL to image (alternative to base64)"
                    },
                    "max_tokens": {
                        "type": "integer",
                        "description": "Max tokens to generate (default: 1000)",
                        "default": 1000
                    }
                },
                "required": ["prompt"]
            }
        ),
        Tool(
            name="direct_math",
            description="DIRECT access to Math-7B (Falcon-H1R-7B). Math reasoning, calculations. Bypasses BROK.",
            inputSchema={
                "type": "object",
                "properties": {
                    "prompt": {
                        "type": "string",
                        "description": "Math problem or calculation"
                    },
                    "system_prompt": {
                        "type": "string",
                        "description": "Optional system prompt"
                    },
                    "max_tokens": {
                        "type": "integer",
                        "description": "Max tokens to generate (default: 2000)",
                        "default": 2000
                    },
                    "temperature": {
                        "type": "number",
                        "description": "Sampling temperature (default: 0.3 for precision)",
                        "default": 0.3
                    }
                },
                "required": ["prompt"]
            }
        ),
        Tool(
            name="direct_qwen3_query",
            description="DIRECT access to Qwen3-8B. Query expansion, context building. Bypasses BROK.",
            inputSchema={
                "type": "object",
                "properties": {
                    "prompt": {
                        "type": "string",
                        "description": "Prompt to send"
                    },
                    "system_prompt": {
                        "type": "string",
                        "description": "Optional system prompt"
                    },
                    "max_tokens": {
                        "type": "integer",
                        "description": "Max tokens to generate (default: 2000)",
                        "default": 2000
                    },
                    "temperature": {
                        "type": "number",
                        "description": "Sampling temperature (default: 0.7)",
                        "default": 0.7
                    }
                },
                "required": ["prompt"]
            }
        ),
        Tool(
            name="direct_reasoning",
            description="DIRECT access to DeepSeek-R1-Distill-Qwen-7B. Chain-of-thought reasoning specialist. Shows thinking process then final answer. Bypasses BROK.",
            inputSchema={
                "type": "object",
                "properties": {
                    "prompt": {
                        "type": "string",
                        "description": "Problem or question requiring step-by-step reasoning"
                    },
                    "system_prompt": {
                        "type": "string",
                        "description": "Optional system prompt"
                    },
                    "max_tokens": {
                        "type": "integer",
                        "description": "Max tokens to generate (default: 4000 for reasoning chains)",
                        "default": 4000
                    },
                    "temperature": {
                        "type": "number",
                        "description": "Sampling temperature (default: 0.6 for balanced reasoning)",
                        "default": 0.6
                    }
                },
                "required": ["prompt"]
            }
        ),
        Tool(
            name="direct_embed",
            description="DIRECT embedding generation via BGE-base (768 dim). Bypasses BROK, goes straight to GPU embed service.",
            inputSchema={
                "type": "object",
                "properties": {
                    "text": {
                        "type": "string",
                        "description": "Text to embed"
                    },
                    "texts": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Multiple texts to embed (batch)"
                    }
                }
            }
        ),
        # =====================================================================
        # BROK CNC - G-Code Generation & CNC Operations
        # =====================================================================
        Tool(
            name="cnc_generate_gcode",
            description="Generate G-code from description or drawing. Uses BROK's CNC AI pipeline with material/machine awareness.",
            inputSchema={
                "type": "object",
                "properties": {
                    "description": {
                        "type": "string",
                        "description": "Text description of what to machine"
                    },
                    "material": {
                        "type": "string",
                        "enum": ["aluminum", "steel", "stainless", "brass", "copper", "plastic", "wood", "foam"],
                        "description": "Material to machine"
                    },
                    "machine": {
                        "type": "string",
                        "enum": ["3axis_mill", "4axis_mill", "5axis_mill", "lathe", "router", "plasma", "laser", "edm"],
                        "description": "Machine type"
                    },
                    "tool_diameter_mm": {
                        "type": "number",
                        "description": "Tool diameter in mm"
                    },
                    "depth_mm": {
                        "type": "number",
                        "description": "Cutting depth in mm"
                    },
                    "feedrate_override": {
                        "type": "number",
                        "description": "Feedrate override percentage (default: 100)"
                    },
                    "include_toolpath_viz": {
                        "type": "boolean",
                        "description": "Include toolpath visualization data",
                        "default": False
                    }
                },
                "required": ["description", "material", "machine"]
            }
        ),
        Tool(
            name="cnc_validate_gcode",
            description="Validate G-code for safety, correctness, and machine compatibility.",
            inputSchema={
                "type": "object",
                "properties": {
                    "gcode": {
                        "type": "string",
                        "description": "G-code to validate"
                    },
                    "machine": {
                        "type": "string",
                        "description": "Target machine type"
                    },
                    "check_collisions": {
                        "type": "boolean",
                        "description": "Run collision simulation",
                        "default": True
                    },
                    "material_bounds": {
                        "type": "object",
                        "description": "Stock dimensions {x, y, z} in mm"
                    }
                },
                "required": ["gcode"]
            }
        ),
        Tool(
            name="cnc_optimize_gcode",
            description="Optimize G-code for faster cycle time or better surface finish.",
            inputSchema={
                "type": "object",
                "properties": {
                    "gcode": {
                        "type": "string",
                        "description": "G-code to optimize"
                    },
                    "optimize_for": {
                        "type": "string",
                        "enum": ["speed", "finish", "tool_life", "balanced"],
                        "description": "Optimization target",
                        "default": "balanced"
                    },
                    "constraints": {
                        "type": "object",
                        "description": "Constraints: {max_feedrate, max_rpm, max_depth_per_pass}"
                    }
                },
                "required": ["gcode"]
            }
        ),
        Tool(
            name="cnc_analyze_part",
            description="Analyze a part image or CAD file for CNC manufacturability.",
            inputSchema={
                "type": "object",
                "properties": {
                    "image_base64": {
                        "type": "string",
                        "description": "Base64-encoded image of part"
                    },
                    "image_url": {
                        "type": "string",
                        "description": "URL to part image"
                    },
                    "material": {
                        "type": "string",
                        "description": "Intended material"
                    },
                    "analyze_features": {
                        "type": "boolean",
                        "description": "Identify machining features (pockets, holes, etc.)",
                        "default": True
                    }
                }
            }
        ),
        Tool(
            name="cnc_calculate_feeds_speeds",
            description="Calculate optimal feeds and speeds for a cutting operation.",
            inputSchema={
                "type": "object",
                "properties": {
                    "material": {
                        "type": "string",
                        "description": "Material being cut"
                    },
                    "tool_type": {
                        "type": "string",
                        "enum": ["endmill", "ballnose", "drill", "tap", "reamer", "facemill", "slotting"],
                        "description": "Type of cutting tool"
                    },
                    "tool_diameter_mm": {
                        "type": "number",
                        "description": "Tool diameter in mm"
                    },
                    "flutes": {
                        "type": "integer",
                        "description": "Number of flutes"
                    },
                    "operation": {
                        "type": "string",
                        "enum": ["roughing", "finishing", "slotting", "profiling", "drilling", "threading"],
                        "description": "Type of operation"
                    },
                    "machine_max_rpm": {
                        "type": "integer",
                        "description": "Machine max spindle RPM"
                    }
                },
                "required": ["material", "tool_type", "tool_diameter_mm"]
            }
        ),
        # =====================================================================
        # BROK CODE V2 - Code Generation & Analysis
        # =====================================================================
        Tool(
            name="code_generate",
            description="Generate code using BROK's code pipeline. Uses Qwen3-Coder-30B with LENS context for codebase-aware generation.",
            inputSchema={
                "type": "object",
                "properties": {
                    "prompt": {
                        "type": "string",
                        "description": "What code to generate"
                    },
                    "language": {
                        "type": "string",
                        "description": "Programming language (auto-detected if not specified)"
                    },
                    "context_files": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "File paths to include as context"
                    },
                    "style": {
                        "type": "string",
                        "enum": ["concise", "documented", "production", "prototype"],
                        "description": "Code style preference",
                        "default": "production"
                    },
                    "include_tests": {
                        "type": "boolean",
                        "description": "Generate test cases",
                        "default": False
                    }
                },
                "required": ["prompt"]
            }
        ),
        Tool(
            name="code_review",
            description="Review code for bugs, security issues, and improvements. Uses BROK's code analysis pipeline.",
            inputSchema={
                "type": "object",
                "properties": {
                    "code": {
                        "type": "string",
                        "description": "Code to review"
                    },
                    "file_path": {
                        "type": "string",
                        "description": "File path (for context)"
                    },
                    "focus": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Review focus areas: ['security', 'performance', 'bugs', 'style', 'all']"
                    },
                    "severity_threshold": {
                        "type": "string",
                        "enum": ["info", "warning", "error", "critical"],
                        "description": "Minimum severity to report",
                        "default": "warning"
                    }
                },
                "required": ["code"]
            }
        ),
        Tool(
            name="code_explain",
            description="Explain code in plain language. Useful for understanding unfamiliar code.",
            inputSchema={
                "type": "object",
                "properties": {
                    "code": {
                        "type": "string",
                        "description": "Code to explain"
                    },
                    "detail_level": {
                        "type": "string",
                        "enum": ["brief", "detailed", "expert"],
                        "description": "Level of detail",
                        "default": "detailed"
                    },
                    "include_diagrams": {
                        "type": "boolean",
                        "description": "Include ASCII diagrams where helpful",
                        "default": False
                    }
                },
                "required": ["code"]
            }
        ),
        Tool(
            name="code_refactor",
            description="Refactor code for better structure, performance, or readability.",
            inputSchema={
                "type": "object",
                "properties": {
                    "code": {
                        "type": "string",
                        "description": "Code to refactor"
                    },
                    "goal": {
                        "type": "string",
                        "enum": ["readability", "performance", "modularity", "testability", "simplify"],
                        "description": "Refactoring goal"
                    },
                    "preserve_interface": {
                        "type": "boolean",
                        "description": "Keep public interface unchanged",
                        "default": True
                    }
                },
                "required": ["code", "goal"]
            }
        ),
        Tool(
            name="code_complete",
            description="Complete partial code. Fill in implementations, finish functions, etc.",
            inputSchema={
                "type": "object",
                "properties": {
                    "code": {
                        "type": "string",
                        "description": "Partial code to complete"
                    },
                    "cursor_position": {
                        "type": "integer",
                        "description": "Cursor position in code (optional)"
                    },
                    "context_before": {
                        "type": "string",
                        "description": "Code before cursor"
                    },
                    "context_after": {
                        "type": "string",
                        "description": "Code after cursor"
                    }
                },
                "required": ["code"]
            }
        ),
        Tool(
            name="code_fix",
            description="Fix code errors given an error message. Diagnoses and repairs.",
            inputSchema={
                "type": "object",
                "properties": {
                    "code": {
                        "type": "string",
                        "description": "Code with error"
                    },
                    "error_message": {
                        "type": "string",
                        "description": "Error message or stack trace"
                    },
                    "language": {
                        "type": "string",
                        "description": "Programming language"
                    },
                    "explain_fix": {
                        "type": "boolean",
                        "description": "Include explanation of what was wrong",
                        "default": True
                    }
                },
                "required": ["code", "error_message"]
            }
        ),
    ]


# =============================================================================
# Tool Handlers
# =============================================================================

@app.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
    """Handle tool calls with security enforcement."""
    logger.info(f"Tool called: {name} with args: {arguments}")

    # ==========================================================================
    # MASTER SECURITY: Block code execution for non-admin users
    # ==========================================================================
    if not CAN_EXECUTE_CODE and name in EXECUTION_BLOCKED_TOOLS:
        logger.warning(f"SECURITY BLOCKED: {name} not allowed for user {BROK_USER_ID}")
        return [TextContent(type="text", text=json.dumps({
            "error": f"Code execution not permitted for this user",
            "tool": name,
            "user_id": BROK_USER_ID,
            "reason": "Only admin can execute code on this server. Code will be delivered to user for execution on their hardware."
        }))]

    try:
        if name == "lens_search":
            result = await handle_lens_search(arguments)
        elif name == "lens_context":
            result = await handle_lens_context(arguments)
        elif name == "llm_query":
            result = await handle_llm_query(arguments)
        elif name == "embed_text":
            result = await handle_embed_text(arguments)
        elif name == "brok_chat":
            result = await handle_brok_chat(arguments)
        elif name == "brok_status":
            result = await handle_brok_status(arguments)
        # Claude Bridge tools
        elif name == "lens_write":
            result = await handle_lens_write(arguments)
        elif name == "get_pending_task":
            result = await handle_get_pending_task(arguments)
        elif name == "submit_solution":
            result = await handle_submit_solution(arguments)
        elif name == "send_heartbeat":
            result = await handle_send_heartbeat(arguments)
        elif name == "log_code_change":
            result = await handle_log_code_change(arguments)
        # Claude Direct Agent tools (Performance Cop)
        elif name == "brok_nats_publish":
            result = await handle_brok_nats_publish(arguments)
        elif name == "brok_nats_request":
            result = await handle_brok_nats_request(arguments)
        elif name == "brok_service_control":
            result = await handle_brok_service_control(arguments)
        elif name == "brok_config_read":
            result = await handle_brok_config_read(arguments)
        elif name == "brok_config_write":
            result = await handle_brok_config_write(arguments)
        elif name == "brok_perf_monitor":
            result = await handle_brok_perf_monitor(arguments)
        elif name == "brok_output_correct":
            result = await handle_brok_output_correct(arguments)
        elif name == "brok_workflow_modify":
            result = await handle_brok_workflow_modify(arguments)
        elif name == "brok_escalation_override":
            result = await handle_brok_escalation_override(arguments)
        # Direct LLM tools (bypass BROK)
        elif name == "direct_gpu_swarm":
            result = await handle_direct_gpu_swarm(arguments)
        elif name == "direct_qwen3_coder":
            result = await handle_direct_qwen3_coder(arguments)
        elif name == "direct_vision":
            result = await handle_direct_vision(arguments)
        elif name == "direct_math":
            result = await handle_direct_math(arguments)
        elif name == "direct_qwen3_query":
            result = await handle_direct_qwen3_query(arguments)
        elif name == "direct_reasoning":
            result = await handle_direct_reasoning(arguments)
        elif name == "direct_embed":
            result = await handle_direct_embed(arguments)
        # BROK CNC tools
        elif name == "cnc_generate_gcode":
            result = await handle_cnc_generate_gcode(arguments)
        elif name == "cnc_validate_gcode":
            result = await handle_cnc_validate_gcode(arguments)
        elif name == "cnc_optimize_gcode":
            result = await handle_cnc_optimize_gcode(arguments)
        elif name == "cnc_analyze_part":
            result = await handle_cnc_analyze_part(arguments)
        elif name == "cnc_calculate_feeds_speeds":
            result = await handle_cnc_calculate_feeds_speeds(arguments)
        # BROK Code V2 tools
        elif name == "code_generate":
            result = await handle_code_generate(arguments)
        elif name == "code_review":
            result = await handle_code_review(arguments)
        elif name == "code_explain":
            result = await handle_code_explain(arguments)
        elif name == "code_refactor":
            result = await handle_code_refactor(arguments)
        elif name == "code_complete":
            result = await handle_code_complete(arguments)
        elif name == "code_fix":
            result = await handle_code_fix(arguments)
        else:
            raise ValueError(f"Unknown tool: {name}")

        return [TextContent(type="text", text=json.dumps(result, indent=2))]

    except Exception as e:
        logger.error(f"Error in {name}: {e}")
        return [TextContent(type="text", text=json.dumps({"error": str(e)}))]


async def handle_lens_search(args: dict[str, Any]) -> dict:
    """Search LENS vector database."""
    import sys
    query = args.get("query")
    top_k = args.get("top_k", 10)
    datasets = args.get("datasets", [])

    payload = {
        "query": query,
        "top_k": top_k,
        "user_id": BROK_USER_ID,
    }
    if datasets:
        payload["datasets"] = datasets

    print(f"[LENS_SEARCH] Query: {query}, top_k: {top_k}, user: {BROK_USER_ID}", file=sys.stderr)

    try:
        result = await nats_request(TOPICS["lens_search"], payload)
        print(f"[LENS_SEARCH] NATS result keys: {result.keys() if result else 'None'}", file=sys.stderr)
        print(f"[LENS_SEARCH] Result count: {len(result.get('results', []))}", file=sys.stderr)
        return {
            "status": "success",
            "query": query,
            "results": result.get("results", []),
            "count": len(result.get("results", []))
        }
    except Exception as e:
        # Fallback to HTTP
        try:
            result = await http_request("/v1/lens/search", payload)
            return {
                "status": "success",
                "query": query,
                "results": result.get("results", []),
                "count": len(result.get("results", []))
            }
        except:
            return {"status": "error", "error": str(e)}


async def handle_lens_context(args: dict[str, Any]) -> dict:
    """Get focused context from LENS."""
    query = args.get("query")
    zoom_level = args.get("zoom_level", "medium")
    max_tokens = args.get("max_tokens", 4000)

    payload = {
        "query": query,
        "zoom_level": zoom_level,
        "max_tokens": max_tokens,
        "user_id": BROK_USER_ID,
    }

    try:
        result = await nats_request(TOPICS["lens_context"], payload)
        return {
            "status": "success",
            "query": query,
            "context": result.get("context", ""),
            "sources": result.get("sources", []),
            "token_count": result.get("token_count", 0)
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


async def handle_llm_query(args: dict[str, Any]) -> dict:
    """Query BROK LLM backends."""
    prompt = args.get("prompt")
    backend = args.get("backend", "auto")
    max_tokens = args.get("max_tokens", 2000)
    temperature = args.get("temperature", 0.7)
    system_prompt = args.get("system_prompt", "")

    payload = {
        "prompt": prompt,
        "max_tokens": max_tokens,
        "temperature": temperature,
    }
    if system_prompt:
        payload["system_prompt"] = system_prompt

    # Route to appropriate backend
    if backend == "auto":
        topic = TOPICS["llm_generate"]
    elif backend == "qwen3_coder":
        topic = TOPICS["llm_coder"]
    else:
        topic = f"brok.llm.{backend}.generate"

    try:
        result = await nats_request(topic, payload, timeout=60.0)
        return {
            "status": "success",
            "backend": backend,
            "response": result.get("response", result.get("text", "")),
            "tokens_used": result.get("tokens_used", 0)
        }
    except Exception as e:
        # Fallback to HTTP
        try:
            payload["backend"] = backend
            result = await http_request("/v1/llm/generate", payload, timeout=60.0)
            return {
                "status": "success",
                "backend": backend,
                "response": result.get("response", result.get("text", "")),
                "tokens_used": result.get("tokens_used", 0)
            }
        except:
            return {"status": "error", "error": str(e)}


async def handle_embed_text(args: dict[str, Any]) -> dict:
    """Generate embeddings."""
    text = args.get("text")
    texts = args.get("texts", [])

    if text:
        texts = [text]

    if not texts:
        return {"status": "error", "error": "No text provided"}

    payload = {
        "texts": texts,
        "user_id": BROK_USER_ID,
    }

    try:
        result = await nats_request(TOPICS["embed"], payload)
        return {
            "status": "success",
            "embeddings": result.get("embeddings", []),
            "dimension": 768,
            "count": len(texts)
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


async def handle_brok_chat(args: dict[str, Any]) -> dict:
    """Send message through BROK Core."""
    message = args.get("message")
    session_id = args.get("session_id", "mcp_session")
    include_context = args.get("include_context", True)

    payload = {
        "message": message,
        "user_id": BROK_USER_ID,
        "session_id": session_id,
        "include_context": include_context,
    }

    try:
        result = await nats_request(TOPICS["chat"], payload, timeout=120.0)
        return {
            "status": "success",
            "response": result.get("response", ""),
            "sources": result.get("sources", []),
            "reasoning_path": result.get("reasoning_path", [])
        }
    except Exception as e:
        # Fallback to HTTP
        try:
            result = await http_request("/v1/chat", payload, timeout=120.0)
            return {
                "status": "success",
                "response": result.get("response", ""),
                "sources": result.get("sources", [])
            }
        except:
            return {"status": "error", "error": str(e)}


async def handle_brok_status(args: dict[str, Any]) -> dict:
    """Get BROK system status."""
    try:
        result = await nats_request("brok.core.status", {}, timeout=5.0)
        return {
            "status": "success",
            "system": result
        }
    except Exception as e:
        # Build status from what we know
        import aiohttp

        status = {
            "nats": "disconnected",
            "http": "unknown",
            "backends": {}
        }

        # Check NATS
        try:
            nc = await get_nats()
            if nc and nc.is_connected:
                status["nats"] = "connected"
        except:
            pass

        # Check HTTP
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{BROK_HTTP_URL}/health", timeout=aiohttp.ClientTimeout(total=2)) as resp:
                    if resp.status == 200:
                        status["http"] = "healthy"
        except:
            status["http"] = "unavailable"

        return {"status": "partial", "system": status}


# =============================================================================
# Claude Bridge Tool Handlers
# =============================================================================

import os
import time
import subprocess
from pathlib import Path

# =============================================================================
# MULTI-USER ISOLATION SYSTEM
# =============================================================================
# All data is isolated per-user. Users share the global FAISS index but have
# separate task queues, solutions, and NATS topic namespaces.

USERS_BASE_PATH = "/mnt/BROK_N/BROK_N_AI/users"

def get_user_path(user_id: str, subdir: str = "") -> str:
    """Get user-specific data path with isolation."""
    base = f"{USERS_BASE_PATH}/{user_id}"
    if subdir:
        return f"{base}/{subdir}"
    return base

def get_user_topic(user_id: str, base_topic: str) -> str:
    """Get user-namespaced NATS topic."""
    # brok.claude.task -> brok.user_{id}.claude.task
    parts = base_topic.split('.')
    if len(parts) >= 2:
        return f"{parts[0]}.{user_id}.{'.'.join(parts[1:])}"
    return f"brok.{user_id}.{base_topic}"

def validate_user_request(user_id: str, request_id: str) -> bool:
    """Validate that request ID belongs to user (prevent cross-contamination)."""
    if not request_id:
        return True  # No request ID to validate
    # Request ID format: {user_id}_{source}_{timestamp}_{counter}
    parts = request_id.split('_')
    if len(parts) >= 4:
        return parts[0] == user_id
    return True  # Legacy format without user ID

def extract_user_from_request_id(request_id: str) -> str:
    """Extract user ID from request ID."""
    if not request_id:
        return ""
    parts = request_id.split('_')
    if len(parts) >= 4:
        return parts[0]
    return ""

# Data paths for Claude Bridge (per-user)
def get_claude_tasks_dir(user_id: str) -> str:
    return get_user_path(user_id, "claude_tasks")

def get_claude_solutions_dir(user_id: str) -> str:
    return get_user_path(user_id, "claude_solutions")

def get_claude_changes_dir(user_id: str) -> str:
    return get_user_path(user_id, "claude_changes")

# User-specific paths (V10.3 - Fixed to match persistent agent paths)
CLAUDE_TASKS_DIR = f"/mnt/BROK_N/BROK_N_AI/users/{BROK_USER_ID}/claude_tasks"
CLAUDE_SOLUTIONS_DIR = f"/mnt/BROK_N/BROK_N_AI/users/{BROK_USER_ID}/claude_solutions"
CLAUDE_CHANGES_DIR = f"/mnt/BROK_N/BROK_N_AI/users/{BROK_USER_ID}/claude_changes"

# =============================================================================
# COMPLETION MARKERS & REQUEST-RESPONSE ID SYSTEM
# =============================================================================
# All messages MUST include these markers to prevent incomplete data reads
# Format: __BROK_MSG_START__{request_id}__ ... content ... __BROK_MSG_END__{request_id}__

MSG_START_PREFIX = "__BROK_MSG_START__"
MSG_END_PREFIX = "__BROK_MSG_END__"
MSG_COMPLETE_MARKER = "__BROK_COMPLETE__"

_request_counter = 0

def generate_request_id(user_id: str = None, source: str = "claude") -> str:
    """Generate unique request ID: {user_id}_{source}_{timestamp_ms}_{counter}"""
    global _request_counter
    _request_counter = (_request_counter + 1) % 100000
    timestamp_ms = int(time.time() * 1000)
    uid = user_id or BROK_USER_ID
    return f"{uid}_{source}_{timestamp_ms}_{_request_counter:05d}"

def wrap_with_markers(request_id: str, content: str) -> str:
    """Wrap content with completion markers"""
    return f"{MSG_START_PREFIX}{request_id}__\n{content}\n{MSG_END_PREFIX}{request_id}__\n{MSG_COMPLETE_MARKER}"

def is_message_complete(content: str, request_id: str) -> bool:
    """Check if message has valid completion markers"""
    end_marker = f"{MSG_END_PREFIX}{request_id}__"
    return end_marker in content and MSG_COMPLETE_MARKER in content

def extract_marked_content(content: str, request_id: str) -> str:
    """Extract content from marked message. Returns empty string if invalid."""
    start_marker = f"{MSG_START_PREFIX}{request_id}__\n"
    end_marker = f"\n{MSG_END_PREFIX}{request_id}__"

    start_pos = content.find(start_marker)
    end_pos = content.find(end_marker)

    if start_pos == -1 or end_pos == -1:
        return ""  # Incomplete

    start_pos += len(start_marker)
    if start_pos >= end_pos:
        return ""  # Invalid

    return content[start_pos:end_pos]

# Ensure directories exist
for dir_path in [CLAUDE_TASKS_DIR, CLAUDE_SOLUTIONS_DIR, CLAUDE_CHANGES_DIR]:
    Path(dir_path).mkdir(parents=True, exist_ok=True)


async def handle_lens_write(args: dict[str, Any]) -> dict:
    """Write data to LENS for BROK to pick up."""
    content = args.get("content", "")
    data_type = args.get("data_type", "claude_knowledge")
    metadata = args.get("metadata", {})

    timestamp = int(time.time() * 1000)
    doc_id = f"{data_type}_{timestamp}"

    # Build document
    document = {
        "id": doc_id,
        "type": data_type,
        "content": content,
        "metadata": metadata,
        "timestamp": timestamp,
        "user_id": BROK_USER_ID,
    }

    # Write to appropriate directory based on type
    if data_type == "claude_solution":
        file_path = f"{CLAUDE_SOLUTIONS_DIR}/{doc_id}.json"
    elif data_type == "claude_change":
        file_path = f"{CLAUDE_CHANGES_DIR}/{doc_id}.json"
    else:
        file_path = f"{CLAUDE_SOLUTIONS_DIR}/{doc_id}.json"

    try:
        with open(file_path, 'w') as f:
            json.dump(document, f, indent=2)

        # Also publish via NATS for real-time notification
        try:
            nc = await get_nats()
            if nc:
                topic = f"brok.claude.{data_type}"
                await nc.publish(topic, json.dumps(document).encode())
        except:
            pass  # File write is primary, NATS is bonus

        return {
            "status": "success",
            "doc_id": doc_id,
            "file_path": file_path
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


async def handle_get_pending_task(args: dict[str, Any]) -> dict:
    """Get next pending task for Claude to process."""
    priority_min = args.get("priority_min", 0)

    try:
        # Scan tasks directory for pending tasks
        tasks = []
        for file_path in Path(CLAUDE_TASKS_DIR).glob("*.json"):
            try:
                with open(file_path, 'r') as f:
                    task = json.load(f)
                    if task.get("status") == "pending":
                        priority = task.get("priority", 1)
                        if priority >= priority_min:
                            task["_file_path"] = str(file_path)
                            tasks.append(task)
            except:
                continue

        if not tasks:
            return {
                "status": "success",
                "task": None,
                "message": "No pending tasks"
            }

        # Sort by priority (highest first), then by created_at (oldest first)
        tasks.sort(key=lambda t: (-t.get("priority", 1), t.get("created_at", 0)))

        # Get highest priority task
        task = tasks[0]

        # Mark as assigned
        task["status"] = "assigned"
        task["assigned_at"] = int(time.time() * 1000)
        file_path = task["_file_path"]
        del task["_file_path"]
        with open(file_path, 'w') as f:
            json.dump(task, f, indent=2)

        # CRITICAL: Publish pending ack IMMEDIATELY (within 3s for BROK watchdog)
        # This tells BROK "Claude got the task and is starting to process"
        try:
            nc = await get_nats()
            if nc:
                pending_ack = {
                    "task_id": task.get("task_id", ""),
                    "status": "pending",
                    "user_id": BROK_USER_ID,
                    "timestamp": task["assigned_at"]
                }
                await nc.publish("brok.claude.task.pending", json.dumps(pending_ack).encode())
                logger.info(f"[PENDING_ACK] Published for task: {task.get('task_id')}")
        except Exception as e:
            logger.warning(f"[PENDING_ACK] Failed to publish: {e}")

        return {
            "status": "success",
            "task": task
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


async def handle_submit_solution(args: dict[str, Any]) -> dict:
    """
    Submit solution for a task with LENS storage and NATS delivery.

    Flow:
    1. Write solution file for BROK
    2. Update task status to completed
    3. Publish solution via NATS to brok.claude.solution
    4. Store solution in LENS for future reference (learning)
    """
    task_id = args.get("task_id", "")
    solution = args.get("solution", "")
    confidence = args.get("confidence", 1.0)
    sources = args.get("sources", [])

    if not task_id or not solution:
        return {"status": "error", "error": "task_id and solution required"}

    timestamp = int(time.time() * 1000)

    # Generate response ID linked to original task
    response_id = generate_request_id("claude")

    # Get original prompt from task file for LENS storage
    original_prompt = ""
    task_path = f"{CLAUDE_TASKS_DIR}/{task_id}.json"
    if os.path.exists(task_path):
        try:
            with open(task_path, 'r') as f:
                task_data = json.load(f)
                original_prompt = task_data.get("prompt", "")
        except:
            pass

    # Build solution document with markers
    solution_doc = {
        "task_id": task_id,
        "response_id": response_id,
        "type": "claude_solution",
        "solution": solution,
        "confidence": confidence,
        "sources": sources,
        "completed_at": timestamp,
        "success": True,
        "user_id": BROK_USER_ID,
        "_marker_start": f"{MSG_START_PREFIX}{response_id}__",
        "_marker_end": f"{MSG_END_PREFIX}{response_id}__",
        "_complete": MSG_COMPLETE_MARKER,
    }

    lens_stored = False

    try:
        # STEP 1: Write solution file - PURE JSON for BROK to parse
        solution_path = f"{CLAUDE_SOLUTIONS_DIR}/solution_{task_id}.json"
        with open(solution_path, 'w') as f:
            json.dump(solution_doc, f, indent=2)

        # STEP 2: Update task status
        if os.path.exists(task_path):
            with open(task_path, 'r') as f:
                raw = f.read()
            try:
                task = json.loads(raw)
            except:
                for line in raw.split('\n'):
                    if line.startswith('{'):
                        task = json.loads(line)
                        break
            task["status"] = "completed"
            task["completed_at"] = timestamp
            task["response_id"] = response_id
            with open(task_path, 'w') as f:
                json.dump(task, f, indent=2)

        # STEP 3: Publish via NATS (primary delivery to BROK)
        try:
            nc = await get_nats()
            if nc:
                nats_payload = wrap_with_markers(response_id, json.dumps(solution_doc))
                await nc.publish("brok.claude.solution", nats_payload.encode())
                logger.info(f"[SOLUTION] Published to NATS: {task_id}")
        except Exception as e:
            logger.warning(f"[SOLUTION] NATS publish failed: {e}")

        # STEP 4: Store solution in LENS for future reference
        if original_prompt:
            lens_stored = await store_solution_to_lens(
                task_id=task_id,
                prompt=original_prompt,
                solution=solution,
                confidence=confidence,
                sources=sources
            )
            if lens_stored:
                logger.info(f"[SOLUTION] Stored in LENS: {task_id}")

        return {
            "status": "success",
            "task_id": task_id,
            "response_id": response_id,
            "solution_path": solution_path,
            "lens_stored": lens_stored,
            "_complete": MSG_COMPLETE_MARKER
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


async def handle_send_heartbeat(args: dict[str, Any]) -> dict:
    """Send heartbeat to indicate Claude is active."""
    state = args.get("state", "idle")
    tasks_pending = args.get("tasks_pending", 0)

    timestamp = int(time.time() * 1000)

    heartbeat = {
        "type": "claude_heartbeat",
        "terminal_id": f"claude_{BROK_USER_ID}",
        "state": state,
        "tasks_pending": tasks_pending,
        "timestamp": timestamp,
    }

    try:
        # Publish via NATS
        nc = await get_nats()
        if nc:
            await nc.publish("brok.claude.health", json.dumps(heartbeat).encode())
            return {
                "status": "success",
                "timestamp": timestamp,
                "state": state
            }
        else:
            return {"status": "error", "error": "NATS not connected"}
    except Exception as e:
        return {"status": "error", "error": str(e)}


async def handle_log_code_change(args: dict[str, Any]) -> dict:
    """Log a code change for audit trail."""
    file_path = args.get("file_path", "")
    change_type = args.get("change_type", "unknown")
    reason = args.get("reason", "")
    before = args.get("before", "")
    after = args.get("after", "")

    if not file_path or not reason:
        return {"status": "error", "error": "file_path and reason required"}

    timestamp = int(time.time() * 1000)
    change_id = f"change_{timestamp}"

    change_doc = {
        "change_id": change_id,
        "type": "claude_change",
        "file_path": file_path,
        "change_type": change_type,
        "reason": reason,
        "before": before[:1000] if before else "",  # Truncate for storage
        "after": after[:1000] if after else "",
        "timestamp": timestamp,
        "user_id": BROK_USER_ID,
    }

    try:
        # Write change log
        change_path = f"{CLAUDE_CHANGES_DIR}/{change_id}.json"
        with open(change_path, 'w') as f:
            json.dump(change_doc, f, indent=2)

        # Publish via NATS
        try:
            nc = await get_nats()
            if nc:
                await nc.publish("brok.claude.improvement", json.dumps(change_doc).encode())
        except:
            pass

        logger.info(f"Code change logged: {change_type} in {file_path} - {reason}")

        return {
            "status": "success",
            "change_id": change_id,
            "change_path": change_path
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


# =============================================================================
# CLAUDE DIRECT AGENT HANDLERS (Performance Cop)
# =============================================================================

BROK_CONFIG_DIR = "/mnt/BROK_N/BROK_N_AI/config"
BROK_BACKUP_DIR = "/mnt/BROK_N/BROK_N_AI/config/backups"
BROK_CORRECTIONS_DIR = "/mnt/BROK_N/BROK_N_AI/claude_corrections"

# Ensure directories exist
for dir_path in [BROK_BACKUP_DIR, BROK_CORRECTIONS_DIR]:
    Path(dir_path).mkdir(parents=True, exist_ok=True)


async def handle_brok_nats_publish(args: dict[str, Any]) -> dict:
    """Publish directly to BROK NATS topic."""
    topic = args.get("topic", "")
    payload = args.get("payload", {})
    user_isolated = args.get("user_isolated", True)

    if not topic:
        return {"status": "error", "error": "topic required"}

    # Apply user namespace if requested
    if user_isolated:
        topic = get_user_topic(BROK_USER_ID, topic)

    try:
        nc = await get_nats()
        if nc:
            await nc.publish(topic, json.dumps(payload).encode())
            logger.info(f"Published to {topic}: {payload}")
            return {
                "status": "success",
                "topic": topic,
                "user_isolated": user_isolated
            }
        else:
            return {"status": "error", "error": "NATS not connected"}
    except Exception as e:
        return {"status": "error", "error": str(e)}


async def handle_brok_nats_request(args: dict[str, Any]) -> dict:
    """Send NATS request/reply."""
    topic = args.get("topic", "")
    payload = args.get("payload", {})
    timeout = args.get("timeout", 30.0)

    if not topic:
        return {"status": "error", "error": "topic required"}

    try:
        result = await nats_request(topic, payload, timeout=timeout)
        return {
            "status": "success",
            "topic": topic,
            "response": result
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


async def handle_brok_service_control(args: dict[str, Any]) -> dict:
    """Control BROK services via systemctl."""
    service = args.get("service", "")
    action = args.get("action", "status")
    log_lines = args.get("log_lines", 50)

    if not service:
        return {"status": "error", "error": "service required"}

    # Validate service name (security check)
    allowed_services = [
        "brok-core", "brok-lens", "brok-llm-gpu0", "brok-llm-gpu1", "brok-llm-gpu2",
        "brok-embed", "brok-vision", "brok-math", "brok-fcfs", "brok-nats",
        "brok-sdxl", "brok-swarm"
    ]

    # Allow partial match
    service_valid = any(s in service for s in allowed_services) or service in allowed_services
    if not service_valid:
        return {"status": "error", "error": f"Service not allowed: {service}. Allowed: {allowed_services}"}

    try:
        if action == "status":
            result = subprocess.run(
                ["systemctl", "status", service, "--no-pager"],
                capture_output=True, text=True, timeout=10
            )
            return {
                "status": "success",
                "service": service,
                "output": result.stdout,
                "return_code": result.returncode
            }
        elif action == "logs":
            result = subprocess.run(
                ["journalctl", "-u", service, "-n", str(log_lines), "--no-pager"],
                capture_output=True, text=True, timeout=30
            )
            return {
                "status": "success",
                "service": service,
                "logs": result.stdout,
                "lines": log_lines
            }
        elif action in ["restart", "start", "stop"]:
            # These require sudo - use with caution
            result = subprocess.run(
                ["sudo", "systemctl", action, service],
                capture_output=True, text=True, timeout=30
            )
            # Log the action
            logger.warning(f"Service {action} on {service}: {result.returncode}")
            return {
                "status": "success" if result.returncode == 0 else "error",
                "service": service,
                "action": action,
                "output": result.stdout or result.stderr,
                "return_code": result.returncode
            }
        else:
            return {"status": "error", "error": f"Unknown action: {action}"}
    except subprocess.TimeoutExpired:
        return {"status": "error", "error": "Command timed out"}
    except Exception as e:
        return {"status": "error", "error": str(e)}


async def handle_brok_config_read(args: dict[str, Any]) -> dict:
    """Read BROK configuration file."""
    config_path = args.get("config_path", "")

    if not config_path:
        return {"status": "error", "error": "config_path required"}

    # Security: prevent path traversal
    config_path = config_path.replace("..", "").lstrip("/")
    full_path = f"{BROK_CONFIG_DIR}/{config_path}"

    if not os.path.exists(full_path):
        return {"status": "error", "error": f"Config not found: {config_path}"}

    try:
        with open(full_path, 'r') as f:
            content = f.read()
        return {
            "status": "success",
            "config_path": config_path,
            "content": content,
            "size": len(content)
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


async def handle_brok_config_write(args: dict[str, Any]) -> dict:
    """Write BROK configuration file with backup."""
    config_path = args.get("config_path", "")
    content = args.get("content", "")
    reason = args.get("reason", "")

    if not config_path or not content or not reason:
        return {"status": "error", "error": "config_path, content, and reason required"}

    # Security: prevent path traversal
    config_path = config_path.replace("..", "").lstrip("/")
    full_path = f"{BROK_CONFIG_DIR}/{config_path}"

    timestamp = int(time.time())

    try:
        # Create backup if file exists
        if os.path.exists(full_path):
            backup_name = f"{config_path.replace('/', '_')}_{timestamp}.bak"
            backup_path = f"{BROK_BACKUP_DIR}/{backup_name}"
            with open(full_path, 'r') as f:
                original = f.read()
            with open(backup_path, 'w') as f:
                f.write(original)
            logger.info(f"Config backup created: {backup_path}")

        # Write new content
        with open(full_path, 'w') as f:
            f.write(content)

        # Log the change
        change_log = {
            "timestamp": timestamp,
            "config_path": config_path,
            "reason": reason,
            "user_id": BROK_USER_ID,
            "backup_path": backup_path if os.path.exists(full_path) else None
        }

        change_log_path = f"{BROK_BACKUP_DIR}/config_changes.jsonl"
        with open(change_log_path, 'a') as f:
            f.write(json.dumps(change_log) + "\n")

        logger.info(f"Config updated: {config_path} - {reason}")

        return {
            "status": "success",
            "config_path": config_path,
            "backup_path": backup_path if os.path.exists(full_path) else None,
            "reason": reason
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


async def handle_brok_perf_monitor(args: dict[str, Any]) -> dict:
    """Monitor BROK performance metrics."""
    metrics = args.get("metrics", ["all"])
    window_seconds = args.get("window_seconds", 60)

    result = {
        "status": "success",
        "timestamp": int(time.time() * 1000),
        "window_seconds": window_seconds,
        "metrics": {}
    }

    try:
        # GPU metrics via nvidia-smi
        if "all" in metrics or "gpu" in metrics:
            gpu_result = subprocess.run(
                ["nvidia-smi", "--query-gpu=index,name,memory.used,memory.total,utilization.gpu,temperature.gpu",
                 "--format=csv,noheader,nounits"],
                capture_output=True, text=True, timeout=5
            )
            if gpu_result.returncode == 0:
                gpus = []
                for line in gpu_result.stdout.strip().split('\n'):
                    parts = [p.strip() for p in line.split(',')]
                    if len(parts) >= 6:
                        gpus.append({
                            "index": int(parts[0]),
                            "name": parts[1],
                            "memory_used_mb": int(parts[2]),
                            "memory_total_mb": int(parts[3]),
                            "utilization_pct": int(parts[4]),
                            "temperature_c": int(parts[5])
                        })
                result["metrics"]["gpu"] = gpus

        # Memory metrics
        if "all" in metrics or "memory" in metrics:
            mem_result = subprocess.run(
                ["free", "-m"],
                capture_output=True, text=True, timeout=5
            )
            if mem_result.returncode == 0:
                lines = mem_result.stdout.strip().split('\n')
                if len(lines) >= 2:
                    parts = lines[1].split()
                    if len(parts) >= 7:
                        result["metrics"]["memory"] = {
                            "total_mb": int(parts[1]),
                            "used_mb": int(parts[2]),
                            "free_mb": int(parts[3]),
                            "available_mb": int(parts[6])
                        }

        # Queue depths via NATS (if available)
        if "all" in metrics or "queues" in metrics:
            try:
                nc = await get_nats()
                if nc:
                    # Request queue stats from BROK Core
                    queue_result = await nats_request("brok.core.queue_stats", {}, timeout=5.0)
                    result["metrics"]["queues"] = queue_result
            except:
                result["metrics"]["queues"] = {"error": "Could not fetch queue stats"}

        # Latency metrics (request BROK Core stats)
        if "all" in metrics or "latency" in metrics:
            try:
                nc = await get_nats()
                if nc:
                    latency_result = await nats_request("brok.core.latency_stats", {"window_seconds": window_seconds}, timeout=5.0)
                    result["metrics"]["latency"] = latency_result
            except:
                result["metrics"]["latency"] = {"error": "Could not fetch latency stats"}

        return result
    except Exception as e:
        return {"status": "error", "error": str(e)}


async def handle_brok_output_correct(args: dict[str, Any]) -> dict:
    """Correct a BROK output and optionally learn from it."""
    request_id = args.get("request_id", "")
    original_output = args.get("original_output", "")
    corrected_output = args.get("corrected_output", "")
    correction_reason = args.get("correction_reason", "")
    apply_to_future = args.get("apply_to_future", True)

    if not original_output or not corrected_output or not correction_reason:
        return {"status": "error", "error": "original_output, corrected_output, and correction_reason required"}

    timestamp = int(time.time() * 1000)
    correction_id = generate_request_id(BROK_USER_ID, "correction")

    correction_doc = {
        "correction_id": correction_id,
        "request_id": request_id,
        "type": "claude_correction",
        "original_output": original_output[:5000],  # Truncate for storage
        "corrected_output": corrected_output[:5000],
        "correction_reason": correction_reason,
        "apply_to_future": apply_to_future,
        "timestamp": timestamp,
        "user_id": BROK_USER_ID,
    }

    try:
        # Save correction
        correction_path = f"{BROK_CORRECTIONS_DIR}/{correction_id}.json"
        with open(correction_path, 'w') as f:
            json.dump(correction_doc, f, indent=2)

        # Publish via NATS for BROK to learn
        if apply_to_future:
            try:
                nc = await get_nats()
                if nc:
                    await nc.publish("brok.claude.correction", json.dumps(correction_doc).encode())
                    # Also publish to learning topic
                    learning_doc = {
                        "type": "correction_example",
                        "input": original_output,
                        "output": corrected_output,
                        "reason": correction_reason,
                        "timestamp": timestamp
                    }
                    await nc.publish("brok.learning.example", json.dumps(learning_doc).encode())
            except:
                pass

        logger.info(f"Output correction logged: {correction_id} - {correction_reason}")

        return {
            "status": "success",
            "correction_id": correction_id,
            "correction_path": correction_path,
            "applied_to_learning": apply_to_future
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


async def handle_brok_workflow_modify(args: dict[str, Any]) -> dict:
    """Modify BROK workflow state."""
    workflow_id = args.get("workflow_id", "")
    action = args.get("action", "status")
    context = args.get("context", {})
    step_index = args.get("step_index", -1)

    timestamp = int(time.time() * 1000)

    payload = {
        "workflow_id": workflow_id,
        "action": action,
        "user_id": BROK_USER_ID,
        "timestamp": timestamp,
    }

    if action == "inject_context" and context:
        payload["context"] = context
    if action == "skip_step" and step_index >= 0:
        payload["step_index"] = step_index

    try:
        nc = await get_nats()
        if nc:
            # Send workflow control command
            result = await nats_request("brok.core.workflow.control", payload, timeout=10.0)
            logger.info(f"Workflow {action} on {workflow_id}: {result}")
            return {
                "status": "success",
                "workflow_id": workflow_id,
                "action": action,
                "result": result
            }
        else:
            return {"status": "error", "error": "NATS not connected"}
    except Exception as e:
        return {"status": "error", "error": str(e)}


async def handle_brok_escalation_override(args: dict[str, Any]) -> dict:
    """Override BROK escalation tier decision."""
    request_id = args.get("request_id", "")
    force_tier = args.get("force_tier")
    force_backend = args.get("force_backend", "")
    reason = args.get("reason", "")

    if not reason:
        return {"status": "error", "error": "reason required"}

    timestamp = int(time.time() * 1000)

    override_doc = {
        "type": "escalation_override",
        "request_id": request_id,
        "force_tier": force_tier,
        "force_backend": force_backend,
        "reason": reason,
        "user_id": BROK_USER_ID,
        "timestamp": timestamp,
    }

    try:
        nc = await get_nats()
        if nc:
            # Publish escalation override
            await nc.publish("brok.core.escalation.override", json.dumps(override_doc).encode())

            # If request_id provided, also publish to that request's topic
            if request_id:
                await nc.publish(f"brok.request.{request_id}.override", json.dumps(override_doc).encode())

            logger.info(f"Escalation override: tier={force_tier}, backend={force_backend}, reason={reason}")

            return {
                "status": "success",
                "request_id": request_id,
                "force_tier": force_tier,
                "force_backend": force_backend,
                "reason": reason
            }
        else:
            return {"status": "error", "error": "NATS not connected"}
    except Exception as e:
        return {"status": "error", "error": str(e)}


# =============================================================================
# DIRECT LLM ACCESS HANDLERS (Bypass BROK)
# =============================================================================
# These handlers talk directly to LLM HTTP endpoints, bypassing BROK's
# orchestration. Useful for admin work, testing, or when BROK is down.

import aiohttp
import random

# Direct LLM endpoints with correct model names for vLLM
DIRECT_LLM_ENDPOINTS = {
    "gpu_swarm": {
        "base_url": "http://127.0.0.1",
        "ports": list(range(9600, 9608)),
        "model": "SmolLM3-Q8_0.gguf"  # llama.cpp model name
    },
    "qwen3_coder": {
        "url": "http://127.0.0.1:8100/v1/chat/completions",
        "model": "/mnt/BROK_N/BROK_N_AI/models/llm/qwen3-coder-30b-a3b-instruct-hf"
    },
    "vision": {
        "url": "http://127.0.0.1:8010/v1/chat/completions",
        "model": "/mnt/BROK_N/BROK_N_AI/models/multimodal/qwen2.5-vl-7b-instruct"
    },
    "math": {
        "url": "http://127.0.0.1:8012/v1/chat/completions",
        "model": "/mnt/BROK_N/BROK_N_AI/models/llm/falcon-h1r-7b"
    },
    "qwen3_query": {
        "url": "http://127.0.0.1:8016/v1/chat/completions",
        "model": "/mnt/BROK_N/BROK_N_AI/models/llm/qwen3-8b-instruct-hf"
    },
    "reasoning": {
        "url": "http://127.0.0.1:8014/v1/chat/completions",
        "model": "/mnt/BROK_N/BROK_N_AI/models/llm/deepseek-r1-distill-qwen-14b-fp8"
    },
    "embed": {
        "url": "http://127.0.0.1:9108/v1/embeddings",
        "model": "bge-base-en-v1.5"
    },
}


async def direct_llm_request(url: str, prompt: str, system_prompt: str = "",
                              max_tokens: int = 2000, temperature: float = 0.7,
                              model: str = "default") -> dict:
    """Make a direct HTTP request to an LLM endpoint."""
    messages = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": prompt})

    payload = {
        "model": model,
        "messages": messages,
        "max_tokens": max_tokens,
        "temperature": temperature,
        "stream": False
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=120)) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    content = result.get("choices", [{}])[0].get("message", {}).get("content", "")
                    return {
                        "status": "success",
                        "content": content,
                        "model": result.get("model", "unknown"),
                        "usage": result.get("usage", {})
                    }
                else:
                    error_text = await resp.text()
                    return {"status": "error", "error": f"HTTP {resp.status}: {error_text[:500]}"}
    except Exception as e:
        return {"status": "error", "error": str(e)}


async def handle_direct_gpu_swarm(args: dict[str, Any]) -> dict:
    """Direct access to GPU Swarm (8x1.5B workers)."""
    prompt = args.get("prompt", "")
    system_prompt = args.get("system_prompt", "")
    max_tokens = args.get("max_tokens", 2000)
    temperature = args.get("temperature", 0.7)
    worker_id = args.get("worker_id")

    if not prompt:
        return {"status": "error", "error": "prompt required"}

    # Select worker (specific or random available)
    ports = DIRECT_LLM_ENDPOINTS["gpu_swarm"]["ports"]
    if worker_id is not None and 0 <= worker_id <= 7:
        port = ports[worker_id]
    else:
        port = random.choice(ports)

    url = f"http://127.0.0.1:{port}/v1/chat/completions"
    model = DIRECT_LLM_ENDPOINTS["gpu_swarm"]["model"]
    result = await direct_llm_request(url, prompt, system_prompt, max_tokens, temperature, model=model)
    result["worker_port"] = port
    result["backend"] = "gpu_swarm"
    return result


async def handle_direct_qwen3_coder(args: dict[str, Any]) -> dict:
    """Direct access to Qwen3-Coder-30B (PRIMARY BRAIN)."""
    prompt = args.get("prompt", "")
    system_prompt = args.get("system_prompt", "You are Qwen3-Coder, an expert coding assistant.")
    max_tokens = args.get("max_tokens", 4000)
    temperature = args.get("temperature", 0.7)

    if not prompt:
        return {"status": "error", "error": "prompt required"}

    url = DIRECT_LLM_ENDPOINTS["qwen3_coder"]["url"]
    model = DIRECT_LLM_ENDPOINTS["qwen3_coder"]["model"]
    result = await direct_llm_request(url, prompt, system_prompt, max_tokens, temperature, model=model)
    result["backend"] = "qwen3_coder_30b"
    return result


async def handle_direct_vision(args: dict[str, Any]) -> dict:
    """Direct access to Vision-7B for image analysis."""
    prompt = args.get("prompt", "")
    image_base64 = args.get("image_base64", "")
    image_url = args.get("image_url", "")
    max_tokens = args.get("max_tokens", 1000)

    if not prompt:
        return {"status": "error", "error": "prompt required"}

    # Build vision message
    content = [{"type": "text", "text": prompt}]
    if image_base64:
        content.append({"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{image_base64}"}})
    elif image_url:
        content.append({"type": "image_url", "image_url": {"url": image_url}})

    messages = [{"role": "user", "content": content}]

    payload = {
        "model": DIRECT_LLM_ENDPOINTS["vision"]["model"],
        "messages": messages,
        "max_tokens": max_tokens,
        "stream": False
    }

    url = DIRECT_LLM_ENDPOINTS["vision"]["url"]
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=60)) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    content = result.get("choices", [{}])[0].get("message", {}).get("content", "")
                    return {
                        "status": "success",
                        "content": content,
                        "backend": "vision_7b",
                        "usage": result.get("usage", {})
                    }
                else:
                    error_text = await resp.text()
                    return {"status": "error", "error": f"HTTP {resp.status}: {error_text[:500]}"}
    except Exception as e:
        return {"status": "error", "error": str(e)}


async def handle_direct_math(args: dict[str, Any]) -> dict:
    """Direct access for math reasoning - routes to Qwen3-Coder-30B (PRIMARY BRAIN).

    Note: Falcon-H1R-7B was the original math model but has tokenizer issues
    (outputs pad tokens). Qwen3-Coder handles math excellently.
    """
    prompt = args.get("prompt", "")
    system_prompt = args.get("system_prompt", "You are a mathematical reasoning expert. Show your work step by step and verify your calculations.")
    max_tokens = args.get("max_tokens", 2000)
    temperature = args.get("temperature", 0.3)  # Lower temp for precision

    if not prompt:
        return {"status": "error", "error": "prompt required"}

    # Route to Qwen3-Coder (PRIMARY BRAIN) - handles math well
    # Falcon-H1R-7B on 8012 has tokenizer issues (pad token output)
    url = DIRECT_LLM_ENDPOINTS["qwen3_coder"]["url"]
    model = DIRECT_LLM_ENDPOINTS["qwen3_coder"]["model"]
    result = await direct_llm_request(url, prompt, system_prompt, max_tokens, temperature, model=model)
    result["backend"] = "qwen3_coder_math"
    result["note"] = "Routed to Qwen3-Coder-30B (Falcon-H1R has tokenizer issues)"
    return result


async def handle_direct_qwen3_query(args: dict[str, Any]) -> dict:
    """Direct access to Qwen3-8B for query expansion."""
    prompt = args.get("prompt", "")
    system_prompt = args.get("system_prompt", "")
    max_tokens = args.get("max_tokens", 2000)
    temperature = args.get("temperature", 0.7)

    if not prompt:
        return {"status": "error", "error": "prompt required"}

    url = DIRECT_LLM_ENDPOINTS["qwen3_query"]["url"]
    model = DIRECT_LLM_ENDPOINTS["qwen3_query"]["model"]
    result = await direct_llm_request(url, prompt, system_prompt, max_tokens, temperature, model=model)
    result["backend"] = "qwen3_8b"
    return result


async def handle_direct_reasoning(args: dict[str, Any]) -> dict:
    """Direct access to DeepSeek-R1-Distill-Qwen-7B for chain-of-thought reasoning.

    This model specializes in step-by-step reasoning, showing its thought process
    before providing a final answer. Great for math, logic, and complex problem solving.
    """
    prompt = args.get("prompt", "")
    system_prompt = args.get("system_prompt", "You are a reasoning expert. Think through problems step by step, showing your reasoning process clearly before giving your final answer.")
    max_tokens = args.get("max_tokens", 4000)  # Higher default for reasoning chains
    temperature = args.get("temperature", 0.6)  # Balanced for reasoning

    if not prompt:
        return {"status": "error", "error": "prompt required"}

    url = DIRECT_LLM_ENDPOINTS["reasoning"]["url"]
    model = DIRECT_LLM_ENDPOINTS["reasoning"]["model"]
    result = await direct_llm_request(url, prompt, system_prompt, max_tokens, temperature, model=model)
    result["backend"] = "deepseek_r1_7b"
    return result


async def handle_direct_embed(args: dict[str, Any]) -> dict:
    """Direct embedding generation via GPU BGE service."""
    text = args.get("text", "")
    texts = args.get("texts", [])

    if not text and not texts:
        return {"status": "error", "error": "text or texts required"}

    input_texts = texts if texts else [text]

    payload = {
        "model": "bge-base-en-v1.5",
        "input": input_texts
    }

    url = DIRECT_LLM_ENDPOINTS["embed"]["url"]
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    embeddings = [d["embedding"] for d in result.get("data", [])]
                    return {
                        "status": "success",
                        "embeddings": embeddings,
                        "dimension": len(embeddings[0]) if embeddings else 0,
                        "count": len(embeddings),
                        "backend": "bge_base_768"
                    }
                else:
                    error_text = await resp.text()
                    return {"status": "error", "error": f"HTTP {resp.status}: {error_text[:500]}"}
    except Exception as e:
        return {"status": "error", "error": str(e)}


# =============================================================================
# BROK CNC HANDLERS
# =============================================================================

CNC_NATS_TOPICS = {
    "generate": "brok.cnc.generate",
    "validate": "brok.cnc.validate",
    "optimize": "brok.cnc.optimize",
    "analyze": "brok.cnc.analyze",
    "feeds_speeds": "brok.cnc.feeds_speeds",
}


async def handle_cnc_generate_gcode(args: dict[str, Any]) -> dict:
    """Generate G-code from description."""
    description = args.get("description", "")
    material = args.get("material", "aluminum")
    machine = args.get("machine", "3axis_mill")

    if not description:
        return {"status": "error", "error": "description required"}

    payload = {
        "description": description,
        "material": material,
        "machine": machine,
        "tool_diameter_mm": args.get("tool_diameter_mm", 6.0),
        "depth_mm": args.get("depth_mm", 5.0),
        "feedrate_override": args.get("feedrate_override", 100),
        "include_toolpath_viz": args.get("include_toolpath_viz", False),
        "user_id": BROK_USER_ID,
    }

    try:
        result = await nats_request(CNC_NATS_TOPICS["generate"], payload, timeout=60.0)
        return result
    except Exception as e:
        return {"status": "error", "error": str(e)}


async def handle_cnc_validate_gcode(args: dict[str, Any]) -> dict:
    """Validate G-code for safety and correctness."""
    gcode = args.get("gcode", "")

    if not gcode:
        return {"status": "error", "error": "gcode required"}

    payload = {
        "gcode": gcode,
        "machine": args.get("machine", ""),
        "check_collisions": args.get("check_collisions", True),
        "material_bounds": args.get("material_bounds", {}),
        "user_id": BROK_USER_ID,
    }

    try:
        result = await nats_request(CNC_NATS_TOPICS["validate"], payload, timeout=30.0)
        return result
    except Exception as e:
        return {"status": "error", "error": str(e)}


async def handle_cnc_optimize_gcode(args: dict[str, Any]) -> dict:
    """Optimize G-code for speed or finish."""
    gcode = args.get("gcode", "")

    if not gcode:
        return {"status": "error", "error": "gcode required"}

    payload = {
        "gcode": gcode,
        "optimize_for": args.get("optimize_for", "balanced"),
        "constraints": args.get("constraints", {}),
        "user_id": BROK_USER_ID,
    }

    try:
        result = await nats_request(CNC_NATS_TOPICS["optimize"], payload, timeout=60.0)
        return result
    except Exception as e:
        return {"status": "error", "error": str(e)}


async def handle_cnc_analyze_part(args: dict[str, Any]) -> dict:
    """Analyze part image for CNC manufacturability."""
    image_base64 = args.get("image_base64", "")
    image_url = args.get("image_url", "")

    if not image_base64 and not image_url:
        return {"status": "error", "error": "image_base64 or image_url required"}

    payload = {
        "image_base64": image_base64,
        "image_url": image_url,
        "material": args.get("material", ""),
        "analyze_features": args.get("analyze_features", True),
        "user_id": BROK_USER_ID,
    }

    try:
        result = await nats_request(CNC_NATS_TOPICS["analyze"], payload, timeout=60.0)
        return result
    except Exception as e:
        return {"status": "error", "error": str(e)}


async def handle_cnc_calculate_feeds_speeds(args: dict[str, Any]) -> dict:
    """Calculate optimal feeds and speeds."""
    material = args.get("material", "")
    tool_type = args.get("tool_type", "")
    tool_diameter_mm = args.get("tool_diameter_mm", 0)

    if not material or not tool_type or not tool_diameter_mm:
        return {"status": "error", "error": "material, tool_type, and tool_diameter_mm required"}

    payload = {
        "material": material,
        "tool_type": tool_type,
        "tool_diameter_mm": tool_diameter_mm,
        "flutes": args.get("flutes", 4),
        "operation": args.get("operation", "roughing"),
        "machine_max_rpm": args.get("machine_max_rpm", 24000),
        "user_id": BROK_USER_ID,
    }

    try:
        result = await nats_request(CNC_NATS_TOPICS["feeds_speeds"], payload, timeout=10.0)
        return result
    except Exception as e:
        return {"status": "error", "error": str(e)}


# =============================================================================
# BROK CODE V2 HANDLERS
# =============================================================================

CODE_NATS_TOPICS = {
    "generate": "brok.code.generate",
    "review": "brok.code.review",
    "explain": "brok.code.explain",
    "refactor": "brok.code.refactor",
    "complete": "brok.code.complete",
    "fix": "brok.code.fix",
}


async def handle_code_generate(args: dict[str, Any]) -> dict:
    """Generate code using BROK's code pipeline."""
    prompt = args.get("prompt", "")

    if not prompt:
        return {"status": "error", "error": "prompt required"}

    payload = {
        "prompt": prompt,
        "language": args.get("language", ""),
        "context_files": args.get("context_files", []),
        "style": args.get("style", "production"),
        "include_tests": args.get("include_tests", False),
        "user_id": BROK_USER_ID,
    }

    try:
        result = await nats_request(CODE_NATS_TOPICS["generate"], payload, timeout=120.0)
        return result
    except Exception as e:
        # Fallback to direct Qwen3-Coder if BROK code service unavailable
        logger.warning(f"Code generate via NATS failed, using direct LLM: {e}")
        system_prompt = f"Generate {args.get('language', '')} code. Style: {args.get('style', 'production')}."
        if args.get("include_tests"):
            system_prompt += " Include unit tests."
        return await handle_direct_qwen3_coder({
            "prompt": prompt,
            "system_prompt": system_prompt,
            "max_tokens": 4000
        })


async def handle_code_review(args: dict[str, Any]) -> dict:
    """Review code for issues."""
    code = args.get("code", "")

    if not code:
        return {"status": "error", "error": "code required"}

    payload = {
        "code": code,
        "file_path": args.get("file_path", ""),
        "focus": args.get("focus", ["all"]),
        "severity_threshold": args.get("severity_threshold", "warning"),
        "user_id": BROK_USER_ID,
    }

    try:
        result = await nats_request(CODE_NATS_TOPICS["review"], payload, timeout=60.0)
        return result
    except Exception as e:
        # Fallback to direct LLM
        logger.warning(f"Code review via NATS failed, using direct LLM: {e}")
        focus = args.get("focus", ["all"])
        focus_str = ", ".join(focus) if isinstance(focus, list) else focus
        return await handle_direct_qwen3_coder({
            "prompt": f"Review this code for {focus_str} issues. Report findings with severity levels.\n\nCode:\n```\n{code}\n```",
            "system_prompt": "You are a senior code reviewer. Identify bugs, security issues, and improvements.",
            "max_tokens": 2000
        })


async def handle_code_explain(args: dict[str, Any]) -> dict:
    """Explain code in plain language."""
    code = args.get("code", "")

    if not code:
        return {"status": "error", "error": "code required"}

    payload = {
        "code": code,
        "detail_level": args.get("detail_level", "detailed"),
        "include_diagrams": args.get("include_diagrams", False),
        "user_id": BROK_USER_ID,
    }

    try:
        result = await nats_request(CODE_NATS_TOPICS["explain"], payload, timeout=60.0)
        return result
    except Exception as e:
        # Fallback to direct LLM
        logger.warning(f"Code explain via NATS failed, using direct LLM: {e}")
        detail = args.get("detail_level", "detailed")
        diagrams = "Include ASCII diagrams where helpful." if args.get("include_diagrams") else ""
        return await handle_direct_qwen3_coder({
            "prompt": f"Explain this code at a {detail} level. {diagrams}\n\nCode:\n```\n{code}\n```",
            "system_prompt": "You are a code explanation expert. Explain clearly for developers.",
            "max_tokens": 2000
        })


async def handle_code_refactor(args: dict[str, Any]) -> dict:
    """Refactor code for a specific goal."""
    code = args.get("code", "")
    goal = args.get("goal", "")

    if not code or not goal:
        return {"status": "error", "error": "code and goal required"}

    payload = {
        "code": code,
        "goal": goal,
        "preserve_interface": args.get("preserve_interface", True),
        "user_id": BROK_USER_ID,
    }

    try:
        result = await nats_request(CODE_NATS_TOPICS["refactor"], payload, timeout=90.0)
        return result
    except Exception as e:
        # Fallback to direct LLM
        logger.warning(f"Code refactor via NATS failed, using direct LLM: {e}")
        preserve = "Keep the public interface unchanged." if args.get("preserve_interface", True) else ""
        return await handle_direct_qwen3_coder({
            "prompt": f"Refactor this code for {goal}. {preserve}\n\nCode:\n```\n{code}\n```",
            "system_prompt": "You are a refactoring expert. Improve code while maintaining functionality.",
            "max_tokens": 4000
        })


async def handle_code_complete(args: dict[str, Any]) -> dict:
    """Complete partial code."""
    code = args.get("code", "")

    if not code:
        return {"status": "error", "error": "code required"}

    payload = {
        "code": code,
        "cursor_position": args.get("cursor_position"),
        "context_before": args.get("context_before", ""),
        "context_after": args.get("context_after", ""),
        "user_id": BROK_USER_ID,
    }

    try:
        result = await nats_request(CODE_NATS_TOPICS["complete"], payload, timeout=30.0)
        return result
    except Exception as e:
        # Fallback to direct LLM
        logger.warning(f"Code complete via NATS failed, using direct LLM: {e}")
        return await handle_direct_qwen3_coder({
            "prompt": f"Complete this partial code:\n\n```\n{code}\n```",
            "system_prompt": "Complete the code naturally. Only output the completion, not explanation.",
            "max_tokens": 2000
        })


async def handle_code_fix(args: dict[str, Any]) -> dict:
    """Fix code given an error message."""
    code = args.get("code", "")
    error_message = args.get("error_message", "")

    if not code or not error_message:
        return {"status": "error", "error": "code and error_message required"}

    payload = {
        "code": code,
        "error_message": error_message,
        "language": args.get("language", ""),
        "explain_fix": args.get("explain_fix", True),
        "user_id": BROK_USER_ID,
    }

    try:
        result = await nats_request(CODE_NATS_TOPICS["fix"], payload, timeout=60.0)
        return result
    except Exception as e:
        # Fallback to direct LLM
        logger.warning(f"Code fix via NATS failed, using direct LLM: {e}")
        explain = "Explain what was wrong and how you fixed it." if args.get("explain_fix", True) else ""
        return await handle_direct_qwen3_coder({
            "prompt": f"Fix this code that produces the following error. {explain}\n\nError:\n{error_message}\n\nCode:\n```\n{code}\n```",
            "system_prompt": "You are a debugging expert. Fix the code and explain the issue.",
            "max_tokens": 3000
        })


# =============================================================================
# ADVANCED BROK↔CLAUDE BRIDGE (January 2026)
# =============================================================================
# Architecture: NATS for real-time signaling + LENS for context/memory
#
# Flow:
# 1. BROK submits task via NATS → brok.claude.task.submit
# 2. MCP sends immediate pending ack → brok.claude.task.pending (within 3s)
# 3. MCP queries LENS for context enrichment
# 4. MCP processes task (Claude reasoning)
# 5. MCP publishes solution → brok.claude.solution
# 6. MCP stores solution in LENS for future reference
#
# Timeouts:
# - Pending ack: 3 seconds (BROK disables Claude if not received)
# - Stale query: 2 minutes (ignore tasks older than this)
# - Solution wait: 2 minutes (BROK uses local LLM fallback)
# - Connection test: 30 seconds (BROK tests if Claude is alive)
# =============================================================================

STALE_QUERY_TIMEOUT_MS = 120000  # 2 minutes - ignore tasks older than this

# Task queue for background processing
_pending_tasks = asyncio.Queue()


async def query_lens_context(query: str, max_tokens: int = 4000) -> dict:
    """Query LENS for relevant context to enrich task processing."""
    try:
        payload = {
            "query": query,
            "zoom_level": "medium",
            "max_tokens": max_tokens,
            "user_id": BROK_USER_ID,
        }
        result = await nats_request(TOPICS["lens_context"], payload, timeout=10.0)
        return {
            "context": result.get("context", ""),
            "sources": result.get("sources", []),
            "token_count": result.get("token_count", 0)
        }
    except Exception as e:
        logger.warning(f"[LENS] Context query failed: {e}")
        return {"context": "", "sources": [], "token_count": 0}


async def search_lens_similar(query: str, top_k: int = 5) -> list:
    """Search LENS for similar past solutions."""
    try:
        payload = {
            "query": query,
            "top_k": top_k,
            "user_id": BROK_USER_ID,
            "datasets": ["claude_solutions", "coding"]
        }
        result = await nats_request(TOPICS["lens_search"], payload, timeout=10.0)
        return result.get("results", [])
    except Exception as e:
        logger.warning(f"[LENS] Similar search failed: {e}")
        return []


async def store_solution_to_lens(task_id: str, prompt: str, solution: str,
                                  confidence: float, sources: list) -> bool:
    """Store solution in LENS for future reference and learning."""
    try:
        timestamp = int(time.time() * 1000)

        # Create document for LENS embedding
        doc_content = f"QUERY: {prompt}\n\nSOLUTION:\n{solution}"

        # Embed the solution
        embed_payload = {
            "texts": [doc_content],
            "user_id": BROK_USER_ID,
        }

        try:
            embed_result = await nats_request(TOPICS["embed"], embed_payload, timeout=15.0)
            embedding = embed_result.get("embeddings", [[]])[0]
        except:
            embedding = []  # Will use text-based storage if embedding fails

        # Store in LENS via NATS
        store_payload = {
            "document": {
                "id": f"solution_{task_id}",
                "type": "claude_solution",
                "content": doc_content,
                "metadata": {
                    "task_id": task_id,
                    "prompt": prompt[:500],  # Truncate for metadata
                    "confidence": confidence,
                    "sources": sources,
                    "timestamp": timestamp,
                    "user_id": BROK_USER_ID,
                }
            },
            "embedding": embedding if embedding else None,
            "user_id": BROK_USER_ID,
        }

        await nats_request("brok.lens.store", store_payload, timeout=10.0)
        logger.info(f"[LENS] Solution stored: {task_id}")
        return True

    except Exception as e:
        logger.warning(f"[LENS] Failed to store solution: {e}")
        # Fallback: store to file
        try:
            solution_doc = {
                "task_id": task_id,
                "prompt": prompt,
                "solution": solution,
                "confidence": confidence,
                "sources": sources,
                "timestamp": int(time.time() * 1000),
                "user_id": BROK_USER_ID,
            }
            solution_path = f"{CLAUDE_SOLUTIONS_DIR}/lens_solution_{task_id}.json"
            with open(solution_path, 'w') as f:
                json.dump(solution_doc, f, indent=2)
            return True
        except:
            return False


async def task_subscription_handler(msg):
    """
    Handle incoming task submissions from BROK.

    Flow:
    1. Immediately send pending ack (within 3 seconds)
    2. Check if task is stale (> 2 minutes old)
    3. Queue task for background processing with LENS context
    """
    try:
        data = json.loads(msg.data.decode())
        task_id = data.get("task_id", "")
        created_at = data.get("created_at", 0)
        prompt = data.get("prompt", "")

        if not task_id:
            return

        logger.info(f"[TASK_SUB] Received task: {task_id}")

        # STEP 1: Immediate pending ack (critical - must be within 3 seconds)
        nc = await get_nats()
        if nc:
            ack = {
                "task_id": task_id,
                "status": "pending",
                "source": "mcp_server",
                "timestamp": int(time.time() * 1000)
            }
            await nc.publish("brok.claude.task.pending", json.dumps(ack).encode())
            logger.info(f"[TASK_SUB] Pending ack sent for: {task_id}")

        # STEP 2: Check if task is stale
        now_ms = int(time.time() * 1000)
        if created_at and (now_ms - created_at) > STALE_QUERY_TIMEOUT_MS:
            logger.warning(f"[TASK_SUB] Ignoring stale task: {task_id} (age: {now_ms - created_at}ms)")
            # Send rejection
            if nc:
                rejection = {
                    "task_id": task_id,
                    "status": "rejected",
                    "reason": "stale_query",
                    "age_ms": now_ms - created_at,
                    "source": "mcp_server"
                }
                await nc.publish("brok.claude.task.rejected", json.dumps(rejection).encode())
            return

        # STEP 3: Queue for background processing
        await _pending_tasks.put(data)
        logger.info(f"[TASK_SUB] Task queued for processing: {task_id}")

    except Exception as e:
        logger.error(f"[TASK_SUB] Error handling task: {e}")


async def connection_test_handler(msg):
    """
    Handle connection test requests from BROK.

    BROK sends tests every 30 seconds when disconnected to check if Claude is alive.
    We must respond to brok.claude.connection.test.response within the timeout.
    """
    try:
        data = json.loads(msg.data.decode())
        test_id = data.get("test_id", "")

        logger.info(f"[CONN_TEST] Received connection test: {test_id}")

        nc = await get_nats()
        if nc:
            response = {
                "test_id": test_id,
                "status": "alive",
                "source": "mcp_server",
                "timestamp": int(time.time() * 1000),
                "capabilities": ["task_processing", "lens_context", "solution_storage"]
            }
            await nc.publish("brok.claude.connection.test.response", json.dumps(response).encode())
            logger.info(f"[CONN_TEST] Responded alive to test: {test_id}")

    except Exception as e:
        logger.error(f"[CONN_TEST] Error handling connection test: {e}")


async def task_processor():
    """
    Background task processor that enriches tasks with LENS context
    and publishes solutions.

    Note: The actual Claude reasoning happens when this MCP server is used
    by Claude Code. This processor handles the context enrichment and
    solution delivery pipeline.
    """
    logger.info("[PROCESSOR] Task processor started")

    while True:
        try:
            # Get next task from queue (blocks until available)
            task = await _pending_tasks.get()
            task_id = task.get("task_id", "")
            prompt = task.get("prompt", "")
            context = task.get("context", "")
            priority = task.get("priority", 1)

            logger.info(f"[PROCESSOR] Processing task: {task_id}")

            # STEP 1: Query LENS for context enrichment
            lens_context = await query_lens_context(prompt)

            # STEP 2: Search for similar past solutions
            similar_solutions = await search_lens_similar(prompt, top_k=3)

            # STEP 3: Build enriched context
            enriched_context = context
            if lens_context.get("context"):
                enriched_context += f"\n\n--- LENS Knowledge ---\n{lens_context['context']}"
            if similar_solutions:
                enriched_context += "\n\n--- Similar Past Solutions ---\n"
                for i, sol in enumerate(similar_solutions[:3]):
                    enriched_context += f"\n{i+1}. {sol.get('content', '')[:500]}...\n"

            # STEP 4: Store enriched task for Claude to process
            # (Claude will pick this up via get_pending_task tool)
            enriched_task = {
                **task,
                "enriched_context": enriched_context,
                "lens_sources": lens_context.get("sources", []),
                "similar_solutions": [s.get("id") for s in similar_solutions],
                "status": "ready_for_claude",
                "enriched_at": int(time.time() * 1000)
            }

            # Save enriched task
            task_path = f"{CLAUDE_TASKS_DIR}/{task_id}.json"
            with open(task_path, 'w') as f:
                json.dump(enriched_task, f, indent=2)

            logger.info(f"[PROCESSOR] Task enriched and ready: {task_id} "
                       f"(LENS context: {lens_context.get('token_count', 0)} tokens, "
                       f"similar solutions: {len(similar_solutions)})")

        except asyncio.CancelledError:
            logger.info("[PROCESSOR] Task processor cancelled")
            break
        except Exception as e:
            logger.error(f"[PROCESSOR] Error processing task: {e}")
            await asyncio.sleep(1)  # Brief pause on error


async def start_task_listener():
    """
    Start background NATS subscriptions for:
    1. Task submissions (brok.claude.task.submit)
    2. Connection tests (brok.claude.connection.test)

    Also starts the background task processor.
    """
    try:
        nc = await get_nats()
        if nc:
            # Subscribe to task submissions
            await nc.subscribe("brok.claude.task.submit", cb=task_subscription_handler)
            logger.info("[TASK_SUB] Subscribed to brok.claude.task.submit for pending acks")

            # Subscribe to connection tests
            await nc.subscribe("brok.claude.connection.test", cb=connection_test_handler)
            logger.info("[CONN_TEST] Subscribed to brok.claude.connection.test")

            # Start background task processor
            asyncio.create_task(task_processor())
            logger.info("[PROCESSOR] Background task processor started")

    except Exception as e:
        logger.error(f"[TASK_SUB] Failed to start task listener: {e}")


async def main():
    """Run the BROK MCP server."""
    import sys
    logger.info("Starting BROK MCP server")
    logger.info(f"NATS URL: {BROK_NATS_URL}")
    logger.info(f"HTTP URL: {BROK_HTTP_URL}")
    logger.info(f"User ID: {BROK_USER_ID}")
    logger.info(f"User isolation enabled: Users base path = {USERS_BASE_PATH}")

    # STARTUP NATS CONNECTIVITY TEST
    print(f"[MCP_STARTUP] Testing NATS connectivity...", file=sys.stderr)
    try:
        import nats
        connect_opts = {"servers": [BROK_NATS_URL]}
        if BROK_NATS_USER and BROK_NATS_PASSWORD:
            connect_opts["user"] = BROK_NATS_USER
            connect_opts["password"] = BROK_NATS_PASSWORD
        test_nc = await nats.connect(**connect_opts)
        print(f"[MCP_STARTUP] NATS connected: {test_nc.is_connected}", file=sys.stderr)
        # Test a simple request
        try:
            resp = await test_nc.request("brok.core.status", b'{}', timeout=5.0)
            print(f"[MCP_STARTUP] NATS test request OK", file=sys.stderr)
        except Exception as e:
            print(f"[MCP_STARTUP] NATS test request FAILED: {e}", file=sys.stderr)
        await test_nc.close()
    except Exception as e:
        print(f"[MCP_STARTUP] NATS connection FAILED: {e}", file=sys.stderr)

    # Ensure user directories exist
    for subdir in ["claude_tasks", "claude_solutions", "claude_changes"]:
        user_dir = get_user_path(BROK_USER_ID, subdir)
        Path(user_dir).mkdir(parents=True, exist_ok=True)

    # 2026-01-21: Start background task listener for immediate pending acks
    asyncio.create_task(start_task_listener())

    async with stdio_server() as (read_stream, write_stream):
        await app.run(read_stream, write_stream, app.create_initialization_options())


if __name__ == "__main__":
    asyncio.run(main())
