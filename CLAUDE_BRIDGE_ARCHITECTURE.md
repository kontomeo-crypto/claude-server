# BROK <-> Claude Neuro-Symbolic Bridge Architecture
## Version 2.1 - January 2026

## Overview

This architecture enables seamless bidirectional communication between BROK (neuro-symbolic AI orchestrator) and Claude (advanced reasoning LLM) using:
- **NATS** for real-time messaging and task flow
- **LENS** for context enrichment and knowledge persistence

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           BROK CORE (C++)                                    │
│  ┌─────────────┐  ┌──────────────┐  ┌─────────────┐  ┌──────────────────┐  │
│  │ Escalation  │→ │ Claude Bridge │→ │ Task Queue  │→ │ Solution Handler │  │
│  │ Detector    │  │ (submit_task) │  │ (SQLite)    │  │ (wait_for_sol)   │  │
│  └─────────────┘  └──────────────┘  └─────────────┘  └──────────────────┘  │
│         ↓                ↓                                    ↑              │
│  [Low confidence]  [NATS Publish]                      [NATS Subscribe]      │
│  [No code block]                                                             │
│  [Short response]                                                            │
└─────────────────────────────────────────────────────────────────────────────┘
                           │                                    │
                           ↓                                    ↑
              ┌────────────────────────────────────────────────────────────┐
              │                      NATS (port 42222)                      │
              │  Topics:                                                    │
              │    brok.claude.task.submit    → Task submission             │
              │    brok.claude.task.pending   ← Immediate ack (within 3s)   │
              │    brok.claude.task.rejected  ← Stale task rejection        │
              │    brok.claude.solution       ← Solution delivery           │
              │    brok.claude.health         ↔ Health heartbeat (0.5s)     │
              │    brok.claude.connection.test↔ Connection recovery         │
              └────────────────────────────────────────────────────────────┘
                           │                                    ↑
                           ↓                                    │
┌─────────────────────────────────────────────────────────────────────────────┐
│                        CLAUDE MCP SERVER (Python)                            │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    NATS SUBSCRIPTION LAYER                           │   │
│  │  brok.claude.task.submit     → task_subscription_handler()          │   │
│  │  brok.claude.connection.test → connection_test_handler()            │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                           │                                                  │
│                           ↓                                                  │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────────┐ │
│  │ 1. Pending Ack  │→ │ 2. Stale Check  │→ │ 3. Queue for Processing     │ │
│  │ (instant <3s)   │  │ (>2min = reject)│  │ (_pending_tasks.put())      │ │
│  └─────────────────┘  └─────────────────┘  └─────────────────────────────┘ │
│                                                        │                    │
│                                                        ↓                    │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    BACKGROUND TASK PROCESSOR                         │   │
│  │                                                                       │   │
│  │  ┌──────────────────┐  ┌──────────────────┐  ┌───────────────────┐  │   │
│  │  │ 4. LENS Context  │→ │ 5. Similar Search │→ │ 6. Enrich Task    │  │   │
│  │  │ query_lens_ctx() │  │ search_lens_sim() │  │ (merge context)   │  │   │
│  │  └──────────────────┘  └──────────────────┘  └───────────────────┘  │   │
│  │                                                        │             │   │
│  │                                                        ↓             │   │
│  │  ┌──────────────────────────────────────────────────────────────┐   │   │
│  │  │ 7. Store enriched task → /claude_tasks/{task_id}.json        │   │   │
│  │  │    status: "ready_for_claude"                                 │   │   │
│  │  └──────────────────────────────────────────────────────────────┘   │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    MCP TOOL: submit_solution                         │   │
│  │                                                                       │   │
│  │  ┌──────────────────┐  ┌──────────────────┐  ┌───────────────────┐  │   │
│  │  │ 8. Write JSON    │→ │ 9. NATS Publish  │→ │ 10. LENS Store    │  │   │
│  │  │ solution file    │  │ brok.solution    │  │ store_to_lens()   │  │   │
│  │  └──────────────────┘  └──────────────────┘  └───────────────────┘  │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                           ↓                         ↓                       │
│                    ┌──────────────┐          ┌──────────────┐               │
│                    │    LENS      │          │    LENS      │               │
│                    │ (14M vectors)│          │ (store new   │               │
│                    │ Context/Read │          │  solutions)  │               │
│                    └──────────────┘          └──────────────┘               │
└─────────────────────────────────────────────────────────────────────────────┘

```

## Task Flow (Step by Step)

### 1. Escalation Detection (BROK)
BROK detects when local LLM response is insufficient:
- Response < 100 characters
- Contains "error" or "[Code generation failed]"
- No code block (```) for code requests
- Confidence < 70%

### 2. Task Submission (BROK → Claude)
```json
{
  "task_id": "task_1769009032251_0000",
  "type": "claude_task",
  "prompt": "Write a Rust fibonacci with memoization",
  "context": "Original code request. Local LLM response: ...",
  "priority": 2,
  "reason": "code_generation",
  "user_id": "user123",
  "created_at": 1769009032251,
  "timeout_ms": 120000
}
```

### 3. Immediate Acknowledgment (Claude → BROK)
Within **3 seconds**, Claude MCP server sends:
```json
{
  "task_id": "task_1769009032251_0000",
  "status": "pending",
  "source": "mcp_server",
  "timestamp": 1769009032300
}
```

### 4. Stale Task Check
If `(now - created_at) > 120000ms` (2 minutes):
```json
{
  "task_id": "task_1769009032251_0000",
  "status": "rejected",
  "reason": "stale_query",
  "age_ms": 125000,
  "source": "mcp_server"
}
```

### 5. LENS Context Enrichment (Claude MCP)
Query LENS for relevant knowledge:
- Similar past solutions from `claude_solutions` dataset
- Relevant code snippets from `coding` dataset
- Domain knowledge based on query
- User-specific context

### 6. Task Processing (Claude via MCP Tools)
With enriched context, Claude:
- Analyzes the task using `get_pending_task` tool
- Generates high-quality solution
- Validates code if applicable
- Adds confidence score
- Submits via `submit_solution` tool

### 7. Solution Delivery (Claude → BROK)
```json
{
  "task_id": "task_1769009032251_0000",
  "response_id": "user_1_claude_1769009045000_00001",
  "type": "claude_solution",
  "solution": "```rust\nuse std::collections::HashMap;\n\nfn fibonacci(n: u64, memo: &mut HashMap<u64, u64>) -> u64 {\n    ...\n}\n```",
  "confidence": 0.95,
  "sources": ["lens:rust_examples", "lens:fibonacci_patterns"],
  "completed_at": 1769009045000,
  "success": true,
  "lens_stored": true
}
```

### 8. Solution Storage (Claude → LENS)
Store solution in LENS for future reference:
- Embedded using BGE-base (768 dimensions)
- Stored in `claude_solutions` dataset
- Searchable by future queries
- Builds knowledge base

## Connection State Machine

```
                    ┌──────────────┐
                    │  CONNECTED   │←────────────────┐
                    └──────────────┘                 │
                          │                         │
                    [No pending ack                 │
                     within 3s]                     │
                          ↓                         │
                    ┌──────────────┐          [Connection
                    │ DISCONNECTED │           test passes]
                    └──────────────┘                │
                          │                         │
                    [Every 30s]                     │
                          ↓                         │
                    ┌──────────────┐                │
                    │   TESTING    │────────────────┘
                    └──────────────┘
```

## Timeouts and Recovery

| Event | Timeout | Action on Timeout |
|-------|---------|-------------------|
| Pending Ack | 3 seconds | Disable Claude, use local LLM |
| Solution Wait | 120 seconds | Use local LLM response |
| Connection Test | 30 seconds | Retry connection test |
| Stale Query | 120 seconds | Ignore task (too old) |

## NATS Topics Reference

| Topic | Direction | Purpose |
|-------|-----------|---------|
| `brok.claude.task.submit` | BROK → Claude | Submit task for processing |
| `brok.claude.task.pending` | Claude → BROK | Acknowledge task received |
| `brok.claude.task.rejected` | Claude → BROK | Reject stale task |
| `brok.claude.solution` | Claude → BROK | Deliver completed solution |
| `brok.claude.health` | Claude → BROK | Heartbeat (every 0.5s) |
| `brok.claude.connection.test` | BROK → Claude | Test if Claude is alive |
| `brok.claude.connection.test.response` | Claude → BROK | Confirm alive |

## LENS Data Types

| Type | Purpose | Stored By |
|------|---------|-----------|
| `claude_task` | Pending/completed tasks | BROK |
| `claude_solution` | Solutions from Claude | Claude MCP |
| `claude_heartbeat` | Health timestamps | Claude MCP |
| `claude_change` | Code changes by Claude | Claude MCP |

## MCP Tools for Claude Bridge

| Tool | Purpose |
|------|---------|
| `get_pending_task` | Get next enriched task to process |
| `submit_solution` | Submit solution (NATS + LENS storage) |
| `send_heartbeat` | Send health status to BROK |
| `log_code_change` | Log code modifications for audit |
| `lens_search` | Search LENS for context |
| `lens_context` | Get focused context from LENS |
| `lens_write` | Write knowledge to LENS |

## Files

| File | Purpose |
|------|---------|
| `/mnt/BROK_N/BROK_N_AI/cpp/include/brok_claude_bridge.hpp` | C++ bridge header |
| `/mnt/BROK_N/BROK_N_AI/cpp/src/brok_core.cpp` | Escalation logic |
| `/home/kontomeo/brok-mcp-server/brok_mcp_server.py` | MCP server |
| `/mnt/BROK_N/BROK_N_AI/claude_tasks.db` | Task database |
| `/mnt/BROK_N/BROK_N_AI/claude_tasks/` | Task JSON files |
| `/mnt/BROK_N/BROK_N_AI/claude_solutions/` | Solution JSON files |

## Benefits

1. **Fast Response**: 3-second ack ensures quick fallback
2. **Context-Rich**: LENS provides relevant knowledge
3. **Self-Improving**: Solutions stored for future use
4. **Fault-Tolerant**: Auto-reconnect on failure
5. **Stale Protection**: Old queries ignored
6. **Learning System**: Past solutions improve future responses

## Example: Complete Task Flow

```
1. User asks BROK: "Write a Rust fibonacci with memoization"
2. BROK's local LLM (Qwen3-Coder-30B) generates short response
3. BROK detects: response < 100 chars → ESCALATE TO CLAUDE
4. BROK publishes task to NATS: brok.claude.task.submit
5. Claude MCP receives task, sends pending ack (< 3s)
6. Claude MCP queries LENS for similar solutions
7. Claude MCP enriches task with LENS context
8. Claude processes enriched task
9. Claude submits solution via submit_solution tool
10. Solution published to NATS + stored in LENS
11. BROK receives solution, delivers to user
12. Future fibonacci questions benefit from stored solution
```
