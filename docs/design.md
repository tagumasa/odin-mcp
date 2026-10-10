# odin-mcp design

The specification the implementation governs against: the layer
rules, the two protocol faces, and the memory, threading, and API
discipline every piece follows. The code is the arbiter — where
this document and the code disagree, one of them is defective.

## Layers

```
vendor/odin-jsonrpc  the pinned wire-layer submodule (jsonrpc:
                     collection): both framings, the envelope, the
                     Conn (dispatch and correlation), the outbound
                     writer, the bounded request queue, and its
                     support packages (the cancel-token tree, the
                     monotonic clock)
  vendor/odin-jsonutil  the nested value-layer submodule (jsonutil:
                       collection): JSON value construction,
                       cloning, and freeing
src/mcp              the protocol faces over one version table:
                     the server session and the client wire core
```

`src/mcp` imports the wire layer through the `jsonrpc:` collection,
its value layer through `jsonutil:`, and nothing else beyond
`core:*`. The submodules are never edited here — a bug or an
improvement in either layer lands in the odin-jsonrpc or
odin-jsonutil repository and rides the next pin bump. The cancel
tree is host-facing API on the wire layer's side
(`jsonrpc.conn_call`'s token parameter carries its type); hosts
that need a blocking wait use its `token_wait`.

The split of ownership is a rule, not a convenience:

- **The library owns the protocol**: the version table, the
  method surfaces, the wire half of cancellation.
- **The host owns the process**: transport injection (both faces
  read and write through function pointers, never fds), thread
  policy above the library's own threads, process spawning,
  reconnect backoff, registries, permission, and logging (the
  server's diagnostics ride the `Server.log_fn` hook; there is no
  module-global logger).

## The server face

`Server` is the session state: identity
(`name`/`version`/`description`/`instructions`), three host
callbacks, and the log hook. `server_init` binds `conn.host` to
the server and registers the built-ins:

| method | built-in behavior |
|---|---|
| `initialize` | version negotiation (below), capabilities (`tools.listChanged`), `serverInfo`, `instructions` |
| `notifications/initialized` | latches the atomic `initialized` flag |
| `ping` | an empty result object |
| `tools/list` | serializes the host's `[]Tool_Entry` (`icons` always `[]`, `_meta` always `{}` — shape fixed) |
| `tools/call` | immediate, deferred, or protocol error (below) |
| `notifications/cancelled` | `requestId` → the host's `on_cancel` |

`server_register` / `server_register_notification` add any further
method — the wire layers carry no method allowlist: a host's
`resources/list` registration and the built-ins share one handler
table.

**The error split the 2025-11-25 revision draws**: tool *execution*
failures (bad arguments, missing input) answer as `isError` tool
results — the client surfaces them as content; protocol-level
failures (unknown tool, malformed `tools/call` params, framing
violations) answer as JSON-RPC errors — `-32602` and friends. The
conformance suite checks both sides of the split against the
reference SDK.

**Deferred calls**: `call_tool` returns `deferred = true` and the
host finishes later with `send_tool_response` /
`send_tool_error` (e.g. -32800 when its own cancel table matched
the `notifications/cancelled`). Deferred results carry the reply
deadline, never the notification drop-fast policy — dropping a
`tools/call` result parks the client forever, so a drop past the
deadline is surfaced through the log hook, not passed silently.
`Call_Info` borrows from the request's message arena, which dies
when the handler returns: a deferring host clones anything it still
needs.

## The client face

`Client` is the wire core over a host-filled `Transport` — six
verbs (`read_fn`/`write_fn`/`close_fn`/`kill_fn`/`probe_fn`/
`destroy_fn`) over any byte stream; a spawned child's pipes are the
production form. Process spawning, reconnect backoff, registries,
and secret injection stay host-side on purpose.

Lifecycle, in the host's order:

1. `client_init` (bind transport; the `Client` must stay at one
   address afterwards — the conn's adapters point into it).
2. `client_start` — wires the NDJSON conn and installs the outbound
   writer, which is **mandatory** here: a peer that stops reading
   stdin must block its own writer thread, never a calling turn.
3. `client_start_reader` — the read loop on its own thread; its
   exit fires `on_dead` on that thread.
4. Calls, then the teardown ladder: `client_close` (mark closed,
   release waiters) → the transport's ladder (stdin EOF, bounded
   grace, kill — host policy) → `client_finish` (join the reader,
   destroy the conn, free owned strings). `client_finish` before
   the wire is dead parks forever on the join — the ladder order is
   a contract, and `tools/conformance` runs it end to end.

`Client_Err` is the closed outcome vocabulary: the wire layer's
`Call_Err` outcomes plus `Unsupported_Version` (the peer answered
outside the version table), `Bad_Shape` (the result lacked the
shape the method requires), and `Internal` (the fold's refusal).

Everything rides one generic surface — `client_call(method,
params, arena, deadline, token)` — and the conveniences are thin:
`client_negotiate` (initialize + initialized, stores the negotiated
revision as an owned clone), `client_list_tools` (first page; the
request carries no params at all — the cursor member is
optional-object on the wire, and hosts paginate through
`client_call` with the answered `nextCursor`), `client_call_tool`
(deadline from `call_timeout_ms`, default `CLIENT_CALL_TIMEOUT_MS`
30 s; initialize uses `CLIENT_INIT_TIMEOUT_MS` 10 s) and its render
half `client_render_result` (text contents joined with newlines,
non-text noted, `isError` flagged — the caller gates on `is_error`
alone; the vocabulary carries no success member).

## Protocol versions

One constant table in `src/mcp/protocol.odin` — `PROTOCOL_LATEST`
("2025-11-25") and `PROTOCOL_SUPPORTED`
(2024-11-05, 2025-03-26, 2025-06-18, 2025-11-25) — referenced by
both faces. The policies: the client offers `PROTOCOL_LATEST` (the
specification's rule — send the newest you support), accepts any
table entry, and fails the connection outside the table; the server
answers the requested revision when it is in the table and
`PROTOCOL_LATEST` otherwise. Adding a revision is one edit there.
Spec work re-checks the current revision's changelog on
modelcontextprotocol.io first — the baseline moves.

## Memory and lifetime rules

- Request-scoped arenas for callers, and nothing borrowed across a
  lifetime boundary: whatever must outlive a source arena is
  cloned (the pending slot's copy-in, `clone_value`, the deferred
  host's answer clones), including map keys — Odin maps store key
  headers, so key bytes are the inserter's ownership.
- **Monotonic clocks only**: every deadline and TTL reads the wire
  layer's `platform.mono_ms`.
- **No process globals**: per-session state lives in the
  `Server`/`Client` the host owns.

## API discipline

Naming and vocabulary: multiword types `Word_Word`, procs
snake_case, mutex fields `mu`, allocator fields `allocator` (short
`a` for procedure parameters).
Failure vocabularies are closed per boundary — `_Err` for boundary
enums/unions (`Client_Err`, the wire layer's `Err_Code`), `_Error`
for circumstance-explaining structs — and boundaries convert into
the next vocabulary explicitly at their edge; callers branch on the
member, never on accompanying prose. The client face reports
failures through its own closed `Client_Err`, never through the
wire layer's internal error vocabulary.

## Testing and measurement

- `tests/` — the face suites plus the cross-face tests: this
  library's own client against its own server over an in-memory
  pipe pair, the interop check neither consumer could run in one
  place. `just test` runs them single-threaded under a zero-leak
  log gate; verdicts come from the log, never the exit status
  alone.
- `tools/conformance` — both faces against the official TypeScript
  SDK over real stdio pipes (`just conformance`): the client face
  against a reference SDK server (negotiation, ping, tools
  round-trip, the isError validation shape, `-32602`, cancellation,
  the teardown ladder — every check lives in
  `tools/conformance/main.odin`) and a reference SDK client
  against the server face (the same ladder plus a cancelled
  deferred call leaving a healthy session — its checks live in
  `tools/conformance/sdk_client.cjs`).
- The wire-path benchmarks live in the odin-jsonrpc repository
  with the wire layer.
- The toolchain is pinned (dev-2026-10-nightly:84bc3fc, the frozen
  dev-2026-10 release); CI asserts the pin hard.
