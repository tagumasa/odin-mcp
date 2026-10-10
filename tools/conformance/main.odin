// mcp-conformance: the stdio conformance driver — this library's two
// faces against a reference SDK implementation over real process pipes.
//
//   serve               the binary IS an MCP server on stdio: the minimal
//                       host shape (ndjson reader/writer over stdin/stdout,
//                       the mandatory outbound writer, single-threaded
//                       dispatch through conn_read_loop) with two sample
//                       tools — echo (immediate) and slow (deferred; only
//                       the cancel path ever completes it, with -32800).
//                       A reference SDK client drives it (sdk_client.cjs).
//   client CMD [ARG..]  spawns CMD as an MCP server child and drives it
//                       with the client face: negotiate, ping, tools
//                       list/call, the isError validation shape, the
//                       -32602 unknown-tool protocol error, cancellation,
//                       and the teardown ladder (the sdk_server.cjs
//                       reference server is the expected peer).
//
// A host tool, not part of the hermetic suite: the bounded settle waits
// are real-time by design — only their length bounds wake latency; every
// wire deadline reads the monotonic clock through the library's own
// paths.
package main

import "base:intrinsics"
import "core:encoding/json"
import "core:fmt"
import "core:io"
import "core:mem"
import "core:os"
import "core:strings"
import "core:thread"
import "core:time"

import "jsonrpc:jsonrpc"
import "jsonutil:jsonutil"
import "mcp:mcp"
import "jsonrpc:platform"

SERVE_NAME    :: "odin-mcp-conformance"
SERVE_VERSION :: "0.1.0"

// The deferred-call table bound: a reference host answers overflow as an
// isError result instead of growing the table without bound.
SLOW_PENDING_MAX :: 64

// Settle waits: real-time bounds on host policy, never wire deadlines.
// Budgets read the vendored monotonic clock; only the poll cadence is a
// real-time sleep.
SETTLE_ROUND     :: 150 * time.Millisecond
TEARDOWN_BUDGET  :: i64(5_000)
POLL_ROUND       :: 5 * time.Millisecond

main :: proc() {
	args := os.args
	if len(args) < 2 {
		usage()
		os.exit(2)
	}
	switch args[1] {
	case "serve":
		os.exit(serve_main())
	case "client":
		if len(args) < 3 {
			fmt.eprintln("usage: mcp-conformance client CMD [ARG..]  — CMD is an MCP server over stdio")
			os.exit(2)
		}
		os.exit(client_main(args[2:]))
	case:
		usage()
		os.exit(2)
	}
}

usage :: proc() {
	fmt.eprintln("usage: mcp-conformance serve | mcp-conformance client CMD [ARG..]")
}

// ---------------------------------------------------------------------------
// serve — the minimal stdio host shape
// ---------------------------------------------------------------------------

// Single-threaded dispatch: call_tool and on_cancel both run on the one
// conn_read_loop thread, so the pending table carries no mutex. The table
// is bounded (SLOW_PENDING_MAX) and its string ids are owned clones —
// inserted under a copy, freed on removal and at teardown.
Pending_Call :: struct {
	id:     jsonrpc.Id,
	id_set: bool,
}

Serve_Host :: struct {
	pending:   [dynamic]Pending_Call,
	server:    ^mcp.Server,
	allocator: mem.Allocator,
}

serve_main :: proc() -> int {
	conn := new(jsonrpc.Conn)
	reader: jsonrpc.Reader
	jsonrpc.reader_init_ndjson(&reader, serve_read, nil, jsonrpc.DEFAULT_MAX_FRAME)
	writer: jsonrpc.Writer
	jsonrpc.writer_init_ndjson(&writer, serve_write, nil)
	jsonrpc.conn_init(conn, reader, writer, context.allocator)
	// The outbound writer is mandatory on any host whose client might stop
	// reading: without it a wedged peer parks the dispatch thread in a
	// blocking write.
	if !jsonrpc.conn_start_outbound(conn, jsonrpc.OUTBOUND_FRAMES_CAP, jsonrpc.OUTBOUND_BYTES_CAP) {
		fmt.eprintln("mcp-conformance: cannot start the outbound writer")
		jsonrpc.conn_destroy(conn)
		free(conn)
		return 1
	}

	host := new(Serve_Host)
	host^ = {
		pending   = make([dynamic]Pending_Call, 0, SLOW_PENDING_MAX, context.allocator),
		allocator = context.allocator,
	}

	server := new(mcp.Server)
	server^ = {
		host        = host,
		name        = SERVE_NAME,
		version     = SERVE_VERSION,
		description = "odin-mcp stdio conformance reference host",
		// The Server contract: an owned clone, never a borrowed literal —
		// the unconditional delete at shutdown frees it.
		instructions = strings.clone("", context.allocator),
		list_tools   = serve_list_tools,
		call_tool    = serve_call_tool,
		on_cancel    = serve_on_cancel,
	}
	host.server = server
	mcp.server_init(server, conn) // binds conn.host = server with the registrations

	jsonrpc.conn_read_loop(conn) // stdin EOF (or a framing rejection) ends the session

	// conn_destroy joins the outbound writer, whose unwedging premise is a
	// dead peer (conn.odin states it). serve cannot kill its peer — it is
	// the SDK client's child, not ours — so the premise is carried by the
	// peer's own teardown: the SDK client's close() terminates this
	// process when it is done reading, and its hard-exit timer bounds the
	// session regardless. The join below therefore never outlives the
	// peer's ladder by more than that bound.
	jsonrpc.conn_destroy(conn)
	free(conn)
	delete(server.instructions, context.allocator) // the owned clone from setup
	free(server)
	for p in host.pending {
		serve_pending_free_id(host, p.id)
	}
	delete(host.pending)
	free(host)
	return 0
}

serve_pending_free_id :: proc(h: ^Serve_Host, id: jsonrpc.Id) {
	#partial switch s in id {
	case string:
		delete(s, h.allocator)
	}
}

// core:os maps every failed stdio read (EINTR included) onto the same
// error and drops the errno, so a retry loop is not expressible through
// this API; what matters here is not reporting a hard read error as EOF.
serve_read :: proc(data: rawptr, buf: []u8) -> (int, jsonrpc.Read_Err) {
	_ = data
	n, err := os.read(os.stdin, buf)
	if err == nil {
		if n == 0 {
			return 0, .Eof
		}
		return n, .None
	}
	if err == io.Error.EOF {
		return 0, .Eof
	}
	return 0, .Io
}

serve_write :: proc(data: rawptr, buf: []u8) -> (int, jsonrpc.Read_Err) {
	_ = data
	n, err := os.write(os.stdout, buf)
	if err == nil {
		return n, .None
	}
	return 0, .Io
}

serve_list_tools :: proc(host: rawptr, arena: mem.Allocator) -> []mcp.Tool_Entry {
	_ = host
	entries := make([]mcp.Tool_Entry, 2, arena)

	echo_schema := jsonutil.json_object(3, arena)
	jsonutil.obj_set(&echo_schema, "type", jsonutil.json_string("object"))
	echo_props := jsonutil.json_object(1, arena)
	echo_text := jsonutil.json_object(1, arena)
	jsonutil.obj_set(&echo_text, "type", jsonutil.json_string("string"))
	jsonutil.obj_set_object(&echo_props, "text", echo_text)
	jsonutil.obj_set_object(&echo_schema, "properties", echo_props)
	required := make([]json.Value, 1, arena)
	required[0] = jsonutil.json_string("text")
	jsonutil.obj_set(&echo_schema, "required", jsonutil.json_array(required, arena))

	slow_schema := jsonutil.json_object(2, arena)
	jsonutil.obj_set(&slow_schema, "type", jsonutil.json_string("object"))
	jsonutil.obj_set_object(&slow_schema, "properties", jsonutil.json_object(0, arena))

	entries[0] = mcp.Tool_Entry{
		name           = "echo",
		title          = "Echo",
		description    = "echoes its text argument",
		input_schema   = json.Value(json.Object(echo_schema)),
		read_only_hint = true,
	}
	entries[1] = mcp.Tool_Entry{
		name           = "slow",
		title          = "Slow",
		description    = "never completes on its own; the cancel path answers -32800",
		input_schema   = json.Value(json.Object(slow_schema)),
		read_only_hint = true,
	}
	return entries
}

serve_call_tool :: proc(host: rawptr, call: ^mcp.Call_Info) -> mcp.Call_Outcome {
	h := cast(^Serve_Host)host
	switch call.name {
	case "echo":
		text := ""
		if call.args != nil {
			if v, ok := jsonutil.obj_get(call.args, "text"); ok {
				#partial switch x in v {
				case json.String:
					text = x
				}
			}
		}
		if text == "" {
			// Input validation answers as an isError tool result, never a
			// JSON-RPC -32602.
			return {is_error = true, text = "echo requires a non-empty text argument"}
		}
		// Consumed by tool_result_body on this stack before the frame's
		// temp reset — request-scoped scratch, not a borrow past the send.
		return {text = fmt.aprintf("echo:%s", text, allocator = context.temp_allocator)}

	case "slow":
		if len(h.pending) >= SLOW_PENDING_MAX {
			return {is_error = true, text = "too many deferred slow calls"}
		}
		id := call.id
		#partial switch s in id {
		case string:
			id = strings.clone(s, h.allocator)
		}
		append(&h.pending, Pending_Call{id = id, id_set = call.id_set})
		return {deferred = true}

	case:
		return {
			is_protocol_error = true,
			err_code          = .Invalid_Params,
			err_message       = fmt.aprintf("unknown tool: %s", call.name, allocator = context.temp_allocator),
		}
	}
	return {is_error = true, text = "unreachable"}
}

serve_on_cancel :: proc(host: rawptr, id: jsonrpc.Id) {
	h := cast(^Serve_Host)host
	found := -1
	for p, i in h.pending {
		if id_equal(p.id, id) {
			found = i
			break
		}
	}
	if found < 0 {
		return
	}
	entry := h.pending[found]
	unordered_remove(&h.pending, found)
	mcp.send_tool_error(h.server, entry.id, entry.id_set, .Request_Cancelled, "cancelled", context.temp_allocator)
	serve_pending_free_id(h, entry.id)
}

id_equal :: proc(a, b: jsonrpc.Id) -> bool {
	na, a_num := a.(i64)
	nb, b_num := b.(i64)
	if a_num && b_num {
		return na == nb
	}
	sa, a_str := a.(string)
	sb, b_str := b.(string)
	if a_str && b_str {
		return sa == sb
	}
	return false
}

// ---------------------------------------------------------------------------
// client — the client face against a spawned MCP server
// ---------------------------------------------------------------------------

Child :: struct {
	handle: os.Process,
	in_w:   ^os.File, // parent's write end: the child's stdin
	out_r:  ^os.File, // parent's read end: the child's stdout
}

child_spawn :: proc(cmd: []string) -> (child: os.Process, in_w, out_r: ^os.File, ok: bool) {
	in_r, w, perr := os.pipe()
	if perr != nil {
		fmt.eprintln("mcp-conformance: cannot pipe the child's stdin:", perr)
		return
	}
	r, out_w, perr2 := os.pipe()
	if perr2 != nil {
		fmt.eprintln("mcp-conformance: cannot pipe the child's stdout:", perr2)
		os.close(in_r)
		os.close(w)
		return
	}
	desc := os.Process_Desc{
		command = cmd,
		stdin   = in_r,
		stdout  = out_w,
		stderr  = os.stderr,
	}
	started, serr := os.process_start(desc)
	os.close(in_r)
	os.close(out_w)
	if serr != nil {
		fmt.eprintln("mcp-conformance: cannot start the child:", serr)
		os.close(w)
		os.close(r)
		return os.Process{}, nil, nil, false
	}
	return started, w, r, true
}

child_read :: proc(data: rawptr, buf: []u8) -> (int, jsonrpc.Read_Err) {
	c := cast(^Child)data
	n, err := os.read(c.out_r, buf)
	if err == nil {
		if n == 0 {
			return 0, .Eof
		}
		return n, .None
	}
	if err == io.Error.EOF {
		return 0, .Eof
	}
	return 0, .Io
}

child_write :: proc(data: rawptr, buf: []u8) -> (int, jsonrpc.Read_Err) {
	c := cast(^Child)data
	n, err := os.write(c.in_w, buf)
	if err == nil {
		return n, .None
	}
	return 0, .Io
}

child_close :: proc(data: rawptr) {
	c := cast(^Child)data
	if c.in_w != nil {
		os.close(c.in_w) // stdin EOF: the graceful half of the ladder
		c.in_w = nil
	}
}

child_kill :: proc(data: rawptr) {
	c := cast(^Child)data
	if err := os.process_kill(c.handle); err != nil {
		fmt.eprintln("mcp-conformance: the child kill failed:", err)
	}
}

child_probe :: proc(data: rawptr) -> bool {
	c := cast(^Child)data
	_, err := os.process_wait(c.handle, 0)
	return err != .Timeout
}

child_destroy :: proc(data: rawptr) {
	c := cast(^Child)data
	// Bounded reap: a well-behaved peer already exited on its stdin EOF.
	bounded_settle(child_probe, c, TEARDOWN_BUDGET)
	os.close(c.out_r)
	free(c)
}

Dead_Flag :: struct {
	fired: u32,
}

dead_flag_set :: proc(user: rawptr) {
	d := cast(^Dead_Flag)user
	intrinsics.atomic_store(&d.fired, 1)
}

Checks :: struct {
	passed: int,
	failed: int,
}

check :: proc(c: ^Checks, ok: bool, what: string, detail := "") {
	if ok {
		c.passed += 1
		fmt.printfln("ok %2d  %s", c.passed + c.failed, what)
	} else {
		c.failed += 1
		if detail != "" {
			fmt.printfln("FAIL %2d  %s: %s", c.passed + c.failed, what, detail)
		} else {
			fmt.printfln("FAIL %2d  %s", c.passed + c.failed, what)
		}
	}
}

Slow_Result :: struct {
	cerr:   mcp.Client_Err,
	msg:    string, // owned clone on the job's shared allocator
	started: u32,
}

Slow_Job :: struct {
	client: ^mcp.Client,
	res:    ^Slow_Result,
	token:  ^platform.Cancel_Token,
	a:      mem.Allocator, // shared with the joining thread; owns res.msg
}

slow_entry :: proc(job: ^Slow_Job) {
	intrinsics.atomic_store(&job.res.started, 1)
	_, _, msg, cerr := mcp.client_call_tool(job.client, "slow", nil, context.temp_allocator, job.token)
	// The thread's temp allocator dies with the thread (core:thread cleans
	// it at exit) and the caller reads res.msg after the join: the answer
	// crosses on the shared allocator, never the dying temp.
	job.res.msg = strings.clone(msg, job.a)
	job.res.cerr = cerr
}

u32_flag_fired :: proc(data: rawptr) -> bool {
	p := cast(^u32)data
	return intrinsics.atomic_load(p) != 0
}

// bounded_settle parks until pred turns true or the budget (monotonic
// milliseconds) burns; the poll cadence is host policy (real time by
// design).
bounded_settle :: proc(pred: proc(rawptr) -> bool, user: rawptr, budget_ms: i64) -> bool {
	deadline := platform.mono_ms() + budget_ms
	for {
		if pred(user) {
			return true
		}
		if platform.mono_ms() >= deadline {
			return false
		}
		time.sleep(POLL_ROUND)
	}
}

// client_abort tears a partially started client down in the host's
// teardown order: the child dies first (the outbound writer can be parked
// on its stdin — client_finish joins it), then the client's own state
// joins and frees, and only then may the Child itself be freed (the
// writer's conn still reaches it through the transport until that join
// returns). Skips nothing the success ladder runs.
client_abort :: proc(c: ^mcp.Client, child: ^Child) {
	child_close(child)
	child_kill(child)
	mcp.client_finish(c)
	child_destroy(child) // the destroy_fn contract: frees the Child itself
}

client_main :: proc(cmd: []string) -> int {
	child, in_w, out_r, ok := child_spawn(cmd)
	if !ok {
		return 1
	}
	c := new(Child)
	c^ = {handle = child, in_w = in_w, out_r = out_r}

	client: mcp.Client
	mcp.client_init(&client, mcp.Transport{
		read_fn    = child_read,
		write_fn   = child_write,
		close_fn   = child_close,
		kill_fn    = child_kill,
		probe_fn   = child_probe,
		destroy_fn = child_destroy,
		data       = c,
	})
	client.name = "mcp-conformance"
	client.version = SERVE_VERSION
	// The backstop for every scenario, cancellation included: the token
	// path is what should win, this only bounds a hung peer.
	client.call_timeout_ms = 15_000

	checks: Checks
	if cerr := mcp.client_start(&client); cerr != .None {
		fmt.eprintln("mcp-conformance: cannot wire the client conn")
		client_abort(&client, c)
		return 1
	}
	dead := Dead_Flag{}
	client.on_dead = dead_flag_set
	client.hook_user = &dead
	if !mcp.client_start_reader(&client) {
		fmt.eprintln("mcp-conformance: cannot start the client reader")
		client_abort(&client, c)
		return 1
	}

	arena: mem.Dynamic_Arena
	mem.dynamic_arena_init(&arena, context.allocator)
	defer mem.dynamic_arena_destroy(&arena)
	a := mem.dynamic_arena_allocator(&arena)

	msg := ""
	cerr := mcp.Client_Err.None
	msg, cerr = mcp.client_negotiate(&client, a)
	check(&checks, cerr == .None, "initialize negotiates with the reference server", msg)
	check(&checks, client.protocol_version == mcp.PROTOCOL_LATEST, fmt.aprintf("the negotiated revision is %s", mcp.PROTOCOL_LATEST, allocator = a), client.protocol_version)

	res: json.Value
	res, msg, cerr = mcp.client_call(&client, "ping", nil, a, platform.mono_ms() + 5_000)
	check(&checks, cerr == .None && res != nil, "ping answers", msg)

	tools: []mcp.Tool_Info
	tools, msg, cerr = mcp.client_list_tools(&client, a)
	check(&checks, cerr == .None, "tools/list answers", msg)
	has_echo := false
	for t in tools {
		if t.name == "echo" {
			has_echo = t.input_schema != nil
		}
	}
	check(&checks, has_echo, "tools/list carries echo with its inputSchema")

	text := ""
	is_err := false
	echo_params := jsonutil.json_object(1, a)
	jsonutil.obj_set(&echo_params, "text", jsonutil.json_string("conformance"))
	text, is_err, msg, cerr = mcp.client_call_tool(&client, "echo", json.Value(json.Object(echo_params)), a, nil)
	check(&checks, cerr == .None && !is_err && text == "echo:conformance", "tools/call echo round-trips", fmt.aprintf("%s (%s)", text, msg, allocator = a))

	// Input validation fails as an isError tool result, never a protocol
	// error.
	text, is_err, msg, cerr = mcp.client_call_tool(&client, "echo", nil, a, nil)
	check(&checks, cerr == .None && is_err, "missing-argument echo answers isError, not -32602", msg)

	// An unknown tool is a protocol-level -32602, never a tool result.
	text, is_err, msg, cerr = mcp.client_call_tool(&client, "nope", nil, a, nil)
	check(&checks, cerr == .Error_Response, "the unknown tool answers -32602", msg)

	// Cancellation: the slow tool sleeps a minute; the token cuts the wait
	// and notifications/cancelled crosses the wire. The late (aborted)
	// server side sends nothing, and anything it did send would be
	// absorbed — the caller moved on.
	root := new(platform.Cancel_Token, context.allocator)
	platform.token_init_root(root)
	tok := platform.token_derive(root, 0, context.allocator)
	// The slow answer crosses threads (worker writes, main reads after
	// the join): a mutex allocator serializes its clone/free against the
	// main thread's own allocating while it polls.
	slow_ma: mem.Mutex_Allocator
	mem.mutex_allocator_init(&slow_ma, context.allocator)
	slow_a := mem.mutex_allocator(&slow_ma)
	slow_res := new(Slow_Result, context.allocator)
	job := new(Slow_Job, context.allocator)
	job^ = {client = &client, res = slow_res, token = tok, a = slow_a}
	th := thread.create_and_start_with_poly_data(job, slow_entry, self_cleanup = false)
	started := bounded_settle(u32_flag_fired, &slow_res.started, 2_000)
	time.sleep(SETTLE_ROUND) // the request is in flight server-side
	platform.token_fire(tok, .Cancelled)
	thread.join(th)
	free(th, context.allocator)
	check(&checks, started, "the slow call reached the wire")
	check(&checks, slow_res.cerr == .Cancelled, "cancellation returns Cancelled inside the deadline", slow_res.msg)
	if len(slow_res.msg) > 0 {
		delete(slow_res.msg, slow_a)
	}

	// Teardown ladder: close the conn, then the child's stdin; the reader
	// sees the wire die; a peer that missed the EOF is killed; everything
	// joins and frees in the host's order.
	mcp.client_close(&client)
	child_close(c)
	exited := bounded_settle(child_probe, c, TEARDOWN_BUDGET)
	if !exited {
		child_kill(c)
	}
	mcp.client_finish(&client)
	child_destroy(c) // the destroy_fn contract: frees the Child itself
	check(&checks, exited, "the child exited on stdin EOF")
	check(&checks, dead.fired != 0, "the reader observed the wire going dead")

	platform.token_destroy(tok, context.allocator)
	platform.token_destroy(root, context.allocator)
	free(job, context.allocator)
	free(slow_res, context.allocator)

	fmt.printfln("%s: %d ok, %d failed", checks.failed == 0 ? "PASS" : "FAIL", checks.passed, checks.failed)
	return 0 if checks.failed == 0 else 1
}
