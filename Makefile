SHELL := /bin/bash

.PHONY: demo demo-clean

demo:
	@set -euo pipefail; \
	export MACP_TOKEN=dev-token; \
	mkdir -p demo; \
	if [ ! -f demo/demo.txt ]; then echo -e "A\nB\nC" > demo/demo.txt; fi; \
	python -m hub.app & echo $$! > .macp/hub.pid; \
	sleep 0.5; \
	python -m bridges.openai_adapter & echo $$! > .macp/openai_adapter.pid; \
	# Optional: start MCP bridge (stdio) in background; safe to ignore if not installed
	( python -m bridges.mcp_server >/dev/null 2>&1 & echo $$! > .macp/mcp_server.pid ) || true; \
	# Stream system events to console
	(macp subscribe --topic system &) ; \
	# Run agents sequentially to demonstrate lock + conflict
	python agents/agent_a.py; \
	python agents/agent_b.py; \
	# Run tests and stream result summary
	macp test --show-logs; \
	$(MAKE) demo-clean

demo-clean:
	@-kill $$(cat .macp/hub.pid 2>/dev/null) 2>/dev/null || true; rm -f .macp/hub.pid;
	@-kill $$(cat .macp/openai_adapter.pid 2>/dev/null) 2>/dev/null || true; rm -f .macp/openai_adapter.pid;
	@-kill $$(cat .macp/mcp_server.pid 2>/dev/null) 2>/dev/null || true; rm -f .macp/mcp_server.pid;
	@true

