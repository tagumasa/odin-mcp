# odin-mcp

A Model Context Protocol SDK for Odin: one protocol package
(`src/mcp`) carrying both faces of the protocol — the server session
and the client's wire core — over the
[odin-jsonrpc](https://github.com/tagumasa/odin-jsonrpc) wire layer
(the vendored `vendor/odin-jsonrpc` submodule) and any byte transport
the host injects. Pure Odin on `core:*`, no vendored C.

The specification — layer rules, the two faces, memory and API
discipline — is [docs/design.md](docs/design.md); the wire layer's
own specification lives in the odin-jsonrpc repository.

## Layout

```
src/mcp/           the protocol faces over one version table: the
                   server session and the client wire core
vendor/odin-jsonrpc  the pinned wire-layer submodule (jsonrpc: and
                   its internal support packages — never edited here)
tests/             the face suites plus the cross-face tests — this
                   library's own client against its own server over an
                   in-memory pipe pair
tools/
  conformance/     the two faces vs the official TypeScript SDK over
                   real stdio pipes (just conformance)
docs/              design.md and the documentation index
justfile           the gates: check, test, conformance
.github/           pinned-toolchain CI (Linux, macOS, Windows)
AGENTS.md          rules and policy for coding agents
LICENCE.md         MIT
```

The SDK is general-purpose: what is implemented is the current
surface, not a limit — hosts register further v1-ladder methods
themselves (`server_register` on the server face, `client_call` on
the client face).

## The server face

The host owns the wires: it reads stdin, writes stdout, and injects
both into a `jsonrpc.Conn`. The minimal host shape — the one
`tools/conformance` runs in serve mode:

```odin
conn := new(jsonrpc.Conn)
reader: jsonrpc.Reader
jsonrpc.reader_init_ndjson(&reader, read_fn, nil, jsonrpc.DEFAULT_MAX_FRAME)
writer: jsonrpc.Writer
jsonrpc.writer_init_ndjson(&writer, write_fn, nil)
jsonrpc.conn_init(conn, reader, writer, context.allocator)
// The outbound writer is mandatory on any host whose client might
// stop reading: replies then park in a bounded queue instead of the
// dispatch thread.
jsonrpc.conn_start_outbound(conn, jsonrpc.OUTBOUND_FRAMES_CAP, jsonrpc.OUTBOUND_BYTES_CAP)

server := new(mcp.Server)
server^ = {
	host         = host,
	name         = "my-server",
	version      = "1.0.0",
	instructions = "",
	list_tools   = host_list_tools, // []Tool_Entry on the call's arena
	call_tool    = host_call_tool,  // immediate, deferred, or protocol error
	on_cancel    = host_on_cancel,  // notifications/cancelled → the host's pending table
}
mcp.server_init(server, conn) // binds conn.host = server with the registrations
mcp.server_register(server, "resources/list", host_resources_list) // any v1-ladder method

jsonrpc.conn_read_loop(conn) // dispatch on this thread; stdin EOF ends the session
```

A `tools/call` answers immediately, or returns `deferred = true` and
the host finishes it later with `send_tool_response` /
`send_tool_error` (e.g. -32800 when the call was cancelled) — the
wire half of long-running tools without the library owning workers.

## The client face

The host fills a `Transport` — six verbs over any byte stream; a
spawned child's pipes are the production form — and drives the
lifecycle. Everything above the wire rides one generic surface:

```odin
client: mcp.Client
mcp.client_init(&client, mcp.Transport{
	read_fn = child_read, write_fn = child_write,
	close_fn = child_close, kill_fn = child_kill,
	probe_fn = child_probe, destroy_fn = child_destroy,
	data = child,
})
client.name, client.version = "my-host", "1.0.0"
mcp.client_start(&client)
client.on_dead = host_on_dead // the reader thread's exit: the wire is gone
mcp.client_start_reader(&client)

msg, cerr := mcp.client_negotiate(&client, arena)         // initialize + initialized
tools, msg, cerr := mcp.client_list_tools(&client, arena) // tools conveniences
text, is_err, msg, cerr := mcp.client_call_tool(&client, "name", args, arena, token)

// Every other v1-ladder method rides the same surface:
res, msg, cerr := mcp.client_call(&client, "resources/list", nil, arena, deadline)
```

Teardown is the host's ladder: `client_close`, the child's stdin EOF,
a bounded wait, kill, then `client_finish` — the same order
`tools/conformance` runs. Process spawning, reconnect backoff, and
registries stay host-side on purpose.

## Quick start

```sh
git clone https://github.com/tagumasa/odin-mcp
cd odin-mcp
git submodule update --init   # the vendored odin-jsonrpc wire layer
just check         # odin check -vet -strict-style over the protocol package
just test          # serial suite; verdicts from the log, zero leak lines
just conformance   # both faces vs the TypeScript SDK (see below)
```

`just check` and `just test` are self-contained: the Odin compiler,
`just`, and the submodule are the whole setup. `just conformance`
additionally needs a `@modelcontextprotocol/sdk` install
(`MCP_SDK_NODE_MODULES` points at one; without it the recipe reports
SKIP loudly).

## Requirements

- The Odin compiler, nightly `dev-2026-10-nightly:84bc3fc` (the
  frozen `dev-2026-10` release; the gates are re-run after toolchain
  moves)
- `just` (any recent version); on Windows the `sh`-scripted recipes
  (`test`, `conformance`) need Git Bash's `sh` on PATH — the CI
  runner image carries it
- The `vendor/odin-jsonrpc` submodule initialized
  (`git submodule update --init`)
- Node with the reference SDK, only for `just conformance`

## Conformance

`just conformance` runs both directions over real stdio pipes
against the official TypeScript SDK: the client face against an SDK
reference server (negotiation, ping, tools round-trip, the
isError validation shape, -32602, cancellation, the teardown
ladder) and an SDK reference client against the server face (the
same ladder plus a cancelled deferred call leaving a healthy
session).

## Status

The v1 protocol ladder (through 2025-11-25) is implemented on both
faces.

## Licence

MIT — see [LICENCE.md](LICENCE.md).
