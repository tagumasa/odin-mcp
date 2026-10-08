# odin-mcp build tasks. The protocol faces live under src/ (the
# collection is `mcp:`); the wire layer is the vendored odin-jsonrpc
# submodule (the `jsonrpc:` collection — never edited here, its gates
# run in its own repository); the suites are a separate package in
# tests/ importing both through the collections.

# Odin links .exe on Windows; the tool binaries carry the suffix in
# their names so one recipe line builds and invokes them on every OS.
exe := if os_family() == "windows" { ".exe" } else { "" }

# Show available tasks
default:
    @just --list

# Type-check the protocol package with vet and strict style. The wire
# layer rides in through the jsonrpc collection and is checked with it.
check:
	odin check src/mcp -collection:mcp=src -collection:jsonrpc=vendor/odin-jsonrpc/src -vet -strict-style -no-entry-point

# Run the suites single-threaded under the log gate: the runner's own
# exit status is honored first (a compile failure must not read as a
# green run through an empty log), then the verdict is the grep over
# tmp/test.log (zero errors, zero warnings, zero leak lines). grep -a
# because a failing render test can spill NUL bytes and silently match
# nothing.
test:
	#!/bin/sh
	set -e
	mkdir -p tmp
	set +e
	odin test tests -collection:mcp=src -collection:jsonrpc=vendor/odin-jsonrpc/src -define:ODIN_TEST_THREADS=1 > tmp/test.log 2>&1
	status=$?
	set -e
	cat tmp/test.log
	if [ "$status" -ne 0 ]; then
		echo "test runner failed (exit $status; see tmp/test.log)" >&2
		exit "$status"
	fi
	if grep -a -E '\[ERROR\]|\[FATAL\]|\[WARN \]|\[WARN\]|\+\+\+ leak|bad free|out of range' tmp/test.log; then
		echo "test log gate failed (see tmp/test.log)" >&2
		exit 1
	fi

# Conformance: the library's two faces against the official TypeScript
# SDK over real stdio pipes (tools/conformance). Not part of `just
# test` — it needs a reference SDK install; MCP_SDK_NODE_MODULES points
# at one, otherwise an npm cache carrying @modelcontextprotocol/sdk is
# probed and the recipe reports SKIP loudly when neither exists.
conformance:
	#!/bin/sh
	set -e
	sdk_nm="${MCP_SDK_NODE_MODULES:-}"
	if [ -z "$sdk_nm" ]; then
		for d in "$HOME"/.npm/_npx/*/node_modules; do
			if [ -d "$d/@modelcontextprotocol/sdk" ]; then sdk_nm="$d"; break; fi
		done
	fi
	if [ -z "$sdk_nm" ]; then
		echo "SKIP: no @modelcontextprotocol/sdk install found; set MCP_SDK_NODE_MODULES to run conformance" >&2
		exit 0
	fi
	mkdir -p bin
	odin build tools/conformance -collection:mcp=src -collection:jsonrpc=vendor/odin-jsonrpc/src -vet -strict-style -o:speed -out:bin/mcp-conformance{{exe}}
	echo "== conformance: the client face vs the reference SDK server =="
	NODE_PATH="$sdk_nm" ./bin/mcp-conformance{{exe}} client node tools/conformance/sdk_server.cjs
	echo "== conformance: the reference SDK client vs the server face =="
	NODE_PATH="$sdk_nm" node tools/conformance/sdk_client.cjs ./bin/mcp-conformance{{exe}} serve
	echo "conformance: both directions green"
