# CodeLoom — MACP Hub

Local‑first Multi‑Agent Coding Protocol (MACP) hub coordinating coding agents (Claude Code via MCP, OpenAI Codex via Responses tools) with a typed message bus, file locks, unified diff apply, project state, and streaming events.

Features

- Transports: WebSocket JSON‑RPC on `ws://127.0.0.1:8765`; HTTP health + SSE at `http://127.0.0.1:8080`.
- Auth: `Authorization: Bearer <token>` required for WS handshake and all HTTP endpoints except `/health`. Dev mode allows if no token set.
- Contracts: JSON Schemas in `schemas/` and strict Pydantic models in `macp_types/` (unknown fields denied).
- Pub/Sub: in‑proc bus with topics `system`, `task:<id>`, and `file:<path>`; SSE streaming with initial replay.
- Project State: in‑memory store with append‑only journal at `.macp/state.journal` for file revs, diffs, and tasks.
- Locks: FIFO locks supporting whole‑file and line‑range scopes with grant/release events and fair queue handoffs.
- Diff.apply: unified diff apply with atomic writes and revision bump; conflict error on mismatch. With a range lock, all hunks must fall within the granted range or a structured "Locked" error is returned.
 - I/O durability: atomic writes fsync the temp file and the containing directory after
   `os.replace` to prevent file loss after power failure; journal entries are flushed and
   fsync'ed per line; snapshots are written via temp + rename with a directory fsync.
- Tests.run: `pytest` runner with safe arg allowlist; publishes a `TestResult` envelope asynchronously.
- Tasks: `Plan.publish`, `Tasks.list`, `Tasks.get` with task‑scoped event routing via `task_id`.
- Bridges: MCP server (stdio) for Claude Code; OpenAI Responses/Tools adapter (HTTP) for Codex.

Quick Start

- Environment: Python ≥ 3.11. Create venv and install dev deps:
  - `uv venv && source .venv/bin/activate && uv pip install -e ".[dev]"`
  - or `python -m venv .venv && source .venv/bin/activate && pip install -U pip && pip install -e ".[dev]"`
- Start the hub:
  - `export MACP_TOKEN=dev-token`
  - `python -m hub.app`
- Health: `curl http://127.0.0.1:8080/health`
- WebSocket auth: include header `Authorization: Bearer dev-token` when connecting.

Core RPC Methods

- `rpc.ping`: health probe over WS.
- `RegisterAgent`, `DiscoverCapabilities`: basic agent registry and method discovery.
- `Events.publish(envelope)`: validates envelope (JSON Schema + Pydantic), updates Project State for `FileUpdate`, then routes on the bus.
- `Events.subscribe({topics}) -> {stream_token}`: returns an SSE token.
- `FS.list({glob?})`, `FS.read({path})`: read‑only FS operations under repo root.
- `PS.list()`, `PS.get({path})`: list tracked files and get a file record.
- `Lock.request({file, task_id?}) -> {granted, ticket, position}` and `Lock.release({ticket}) -> {ok}`: publishes `LockGrant`/`LockRelease` events (task‑scoped when provided).
- `Diff.apply({file, diff, description, base_rev?, task_id?}) -> {diff_id, new_rev}`: atomic apply; publishes `FileUpdate` (task‑scoped when provided). When applying under a range lock, hunks outside the granted range return a domain error (code `-32004`).
- `Tests.run({runner?, args?, task_id?}) -> {run_id}`: runs pytest, publishes `TestResult` with `correlation_id == run_id`.
- `Plan.publish({task_id, dag?, owners[]}) -> {ok:true}`, `Tasks.list()`, `Tasks.get({task_id})`.

SSE Streaming

- Acquire token: WS `Events.subscribe({"topics":["system", "task:<id>", "file:<path>"]})`.
- Connect: `curl -H "Authorization: Bearer $MACP_TOKEN" "http://127.0.0.1:8080/events?token=<token>"`.
- Events: `heartbeat` and `message` frames. `message.data` contains `{topic, data: <Envelope>}`.

OpenAI Responses/Tools Adapter (Codex)

- Run: `python -m bridges.openai_adapter` (HTTP on `127.0.0.1:8090`).
- Schema: `GET /tools/schema`.
- Call example:
  - `POST /tools/call` with `{ "name": "macp_apply_patch", "arguments": { "file": "README.md", "diff": "--- a\n+++ b\n...", "description": "Update" } }`.
- Tools: `macp_apply_patch`, `macp_run_tests`, `macp_request_lock`, `macp_release_lock`, `macp_publish_message`, `macp_list_files`, `macp_read_file`, `macp_subscribe`.

Subscribe and stream via SSE

- Request a stream token via the adapter:
  - `curl -s -X POST -H 'Content-Type: application/json' \\
      -d '{"name":"macp_subscribe","arguments":{"topics":["system"]}}' \\
      http://127.0.0.1:8090/tools/call`
- The response includes `{ "token": "<...>", "sse_url": "http://127.0.0.1:8080/events?token=<...>" }`.
- Connect your SSE client (include `Authorization: Bearer $MACP_TOKEN`):
  - `curl -H "Authorization: Bearer $MACP_TOKEN" "http://127.0.0.1:8080/events?token=<token>"`

Claude Code MCP Bridge

- Location: `bridges/mcp_server.py` (stdio MCP server exposing MACP tools/resources).
- Run: `export MACP_TOKEN=dev-token && python -m bridges.mcp_server`.
- Configure Claude Code per Anthropic MCP docs to connect via stdio.

Programmatic Client

- `bridges/hub_client.py` provides an async client for WS JSON‑RPC and SSE.
- See usage in `tests/bridges/test_hub_client.py`.

Security

- Loopback binding (`127.0.0.1`) by default.
- Bearer auth for WS handshake and HTTP/SSE; `/health` is unauthenticated.
- Path allowlist enforced to repo root for FS reads.
- Audit state stored locally in `.macp/state.journal`; no external calls by default.

Auth tokens and allowlists

- Dev mode: set `MACP_TOKEN` (or `CODELOOM_TOKEN`) for a static token.
- Structured tokens: sign `macp1` tokens with `MACP_TOKEN_SECRET` (optionally provide
  `MACP_TOKEN_SECRET_OLD` or a `MACP_TOKEN_SECRET_FILE` with newline‑separated secrets for rotation).
- Payload contains `agent_id`, `allow` (list of globs relative to repo root), and `exp` (epoch seconds).
- When structured tokens are used, the hub enforces per‑agent path allowlists in `FS.read`, `FS.list`, and `Diff.apply`.

Dev & Quality Gates

- Run: `pytest -q`
- Lint/format: `ruff check . && ruff format --check . && black --check .`
- Types: `mypy .`
- Security: `bandit -q -r hub bridges`

Limitations / Roadmap

- Line‑range locks are supported alongside whole‑file locks. 3‑way merges and semantic conflict resolution remain future work.
- Diff engine applies single‑file unified diffs; overlapping hunks cause conflicts (no 3‑way merge yet).
- Token lifecycle (TTL/revocation) and richer rate limiting are pending.
- Optional Git adapter and broader observability remain future work.
