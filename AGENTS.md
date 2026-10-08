# AGENTS.md — odin-mcp

Rules and policy for coding agents on the odin-mcp codebase — not
session records or measurements. User-facing documentation lives in
README.md and docs/.

# Project description

## Overview

odin-mcp is a Model Context Protocol SDK for Odin (pure Odin, `core:*`
only, no C dependencies): `src/mcp` — the protocol types with both
faces over one protocol-version table: the server session and the
client wire core — built on the vendored odin-jsonrpc wire layer
(`vendor/odin-jsonrpc`, the `jsonrpc:` collection: both framings,
the envelope, the Conn, the outbound writer, the request queue, and
its internal support packages). The submodule is never edited here —
a wire-layer bug or improvement lands in the odin-jsonrpc repository
and rides the next pin bump. The client face reports failures
through its own closed `Client_Err`, never through the wire layer's
internal vocabulary.

Hosts own logging (no module-global logger: the server's diagnostics
ride the `Server.log_fn` hook, the client face returns prose with its
errors), process spawning, reconnect backoff, registries, and
permission.

### Package layout and dependency direction

`src/` is a single collection (imports use the `mcp:` prefix); the
wire layer rides in as the `jsonrpc:` collection. Dependencies flow
one way only:

```
vendor/odin-jsonrpc (pinned submodule, jsonrpc: collection —
                     never edited here, its gates run in its own
                     repository)
  ↑ mcp (both protocol faces over one version table)
```

- Lower layers must not import upper layers.
- The protocol-version table is a single constant in `src/mcp`; both
  faces reference it.
- No process-global variables: state rides the owning struct
  (`mcp.Server`, `mcp.Client`); the wire layer's
  `platform.mono_override` is its own reserved seam (a virtual-clock
  test hook, nothing else).

# Project principles

- **Correctness, processing performance, and resource behaviour are the
  product.** A change that trades any of them for convenience is a
  defect. Enforced in: Design rules; Build, test, and measure.
- **The specification governs the implementation.** Where code and
  specification disagree, one of them is defective — determine which,
  and fix that side. Enforced in: Specification.
- **Structure is load-bearing.** Dependencies flow one way, one version
  table serves both faces, and every rule has one home. Enforced in:
  Package layout and dependency direction; Design rules.
- **Every resource has one owner and one bound.** There is no GC safety
  net: lifetimes are explicit, queues are bounded, and ownership
  transfers are visible at the call site. Enforced in: Design rules.
- **Verdicts come from artifacts.** Suite outcomes are read from the
  suite log, never from the exit status alone; measurements carry
  their method and toolchain. Enforced in: Build, test, and measure.
- **Write for the next reader.** Comments state constraints the code
  cannot show; no text cites material the reader does not have.
  Enforced in: Comment self-containment.

## Comment self-containment

Committed code, docs, and error strings must be self-contained: no
citations of private notebooks, decision-record numbers, local machine
paths, or local tool-state directories — every such citation dangles
for readers without the referenced material. State the constraint in
prose where it matters.

Exact numbers that move while the code evolves (suite sizes, default
values, timings) rot between edits; where a magnitude carries a rule,
name the identifier that owns the exact value.

# Development guide

## Build, test, and measure

```sh
git submodule update --init   # the vendored odin-jsonrpc wire layer
just check        # odin check -vet -strict-style -no-entry-point, the protocol package
just test         # odin test tests (ODIN_TEST_THREADS=1), log-gated
just conformance  # both faces vs the TypeScript SDK over real stdio pipes
```

- The Odin compiler is pinned to `dev-2026-10-nightly:84bc3fc` (the
  frozen `dev-2026-10` release; CI asserts the hash). After a toolchain
  move, re-run every gate (and the wire layer's own gates in its
  repository on the same move).
- `just test` writes `tmp/test.log`: the runner's exit status is
  honored first (a compile failure is never a green run), then the
  log gate fails on any match of
  `\[ERROR\]|\[FATAL\]|\[WARN \]|\[WARN\]|\+\+\+ leak|bad free|out of range`.
  The `[WARN ]` alternative is load-bearing (the framework prints its
  per-test leak blocks under a padded header); always `grep -a` (a
  failing test can spill NUL bytes and make grep treat the log as
  binary). The leak discipline is zero leak lines.
- Do NOT use `-define:ODIN_TEST_NAMES` single-test builds — they change
  the binary's layout; the filter exists only as a `-define:` (an
  `ODIN_TEST_NAMES` environment variable does not filter at all).
- `just conformance` needs a `@modelcontextprotocol/sdk` install
  (`MCP_SDK_NODE_MODULES` or a probed npm cache) and reports SKIP
  loudly without one. It is a host tool outside `just test`: its
  bounded settle waits are real time by design, while every wire
  deadline still reads the monotonic clock.
- `tmp/` is recipe scratch and `bin/` holds the built tool binaries —
  both gitignored.
- Commits are conventional style (`feat:`, `fix:`, …); only when the
  user asks.

# Development rules in detail

## Design rules

- **Naming**: multiword types `Word_Word`; procs snake_case; mutex
  fields `mu`; allocator fields `allocator` (short `a` for procedure
  parameters).
- **Error vocabularies are closed per boundary**: `_Err` for boundary
  enums/unions (`Read_Err`, `Call_Err`, `Err_Code`), `_Error` for
  circumstance-explaining structs; failures propagate through explicit
  checks, and the client face reports through `Client_Err`, never
  `platform.Err`.
- **Every queue and cache is bounded**: the outbound writer (frame and
  byte caps) and the server request queue; a full queue is answered
  immediately or drops past its deadline — load is shed at the source,
  never buffered.
- **Memory lifetime is rule-bound**: long-lived maps clone their keys
  on first insert; a value crossing a lifetime boundary is copied into
  the destination allocator; an arena-allocated pointer never escapes
  its scope.
- **Cancellation lives in the cancel-token tree only**
  (`platform.Cancel_Token`): tokens derive from a parent, fire
  propagates downward, waiters poll through the accessors.
- **Monotonic clocks only** for liveness, deadlines, and TTLs
  (`platform.mono_ms`); wall time is display-only.
- **Thread entries go through the typed facade**: every spawn site uses
  `thread.create_and_start_with_poly_data` (`proc(data: $T)`,
  `self_cleanup = false`, the handle freed by the owner) — never the
  rawptr `create_and_start_with_data`. Timer and transport callbacks
  keep the rawptr shape; they are not thread entries.
- **Pure Odin, `core:*` only**: no vendored C in this repository and
  no foreign imports but the wire layer's one darwin libSystem
  `clock_gettime` (inside the submodule — core ships no monotonic
  wrapper there).

## Testing conventions

- Wire deadlines and settle bounds read the monotonic source only.
  Tests settle with bounded real-time polls (1–10 ms sleeps around an
  observed condition: a pipe byte count, an atomic flag, a hook
  counter) — never open-ended spins or unbounded waits.
- `platform.mono_override_set` is the reserved seam for driving
  deadlines from a virtual clock: install before the threads under
  test start, clear after they are joined; no suite wires it.

## Cross-platform

- The justfile's `exe` variable carries the Windows suffix — tool
  binaries build and invoke as `bin/<name>{{exe}}` on every OS.
- The `sh`-scripted recipes (`test`, `conformance`, `bench-callgrind`)
  need Git Bash's `sh` on PATH on Windows; the CI runner image carries
  it.

# Reference

## Odin language and stdlib

- `matrix` and `distinct` are keywords — a field or local by either
  name is a parse error.
- A named return cannot be shadowed by a multi-value local ("Direct
  shadowing of the named return value").
- `mem.Allocator` is the two-field `{procedure, data}` struct.
- A map carries its creating allocator through growth (the growth
  allocation is `.Alloc_Non_Zeroed` on that allocator), and an insert
  whose growth allocation fails is silently dropped — `len` unmoved,
  no error.

## Specification

- The implemented protocol ladder runs through 2025-11-25.
- Before touching anything spec-related, re-check the current
  revision's changelog on modelcontextprotocol.io — the baseline moves.
