// Cross-face tests: this library's own client wire core against its own
// server over the in-memory pipe pair — initialize negotiation (both
// mismatch policies), the tools round-trip, cancellation propagating to
// the peer's notifications/cancelled, tools/list_changed reaching the
// client's hook, and the malformed/oversized frame contracts. This is
// the interop check neither consumer could run in one place.
package tests

import "core:encoding/json"
import "core:mem"
import "core:strings"
import "core:sync"
import "core:testing"
import "core:thread"
import "jsonrpc:jsonrpc"
import "jsonutil:jsonutil"
import "mcp:mcp"
import "jsonrpc:platform"

// --- harness ---------------------------------------------------------------

Cross_Host :: struct {
	server:        ^mcp.Server,
	server_conn:   ^jsonrpc.Conn,
	client:        ^mcp.Client,
	req_pipe:      ^Pipe, // client -> server (the child's stdin side)
	resp_pipe:     ^Pipe, // server -> client (the child's stdout side)
	server_reader: ^thread.Thread,
	reader_box:    ^Cross_Box,
	client_started: bool,

	entries:    []mcp.Tool_Entry,
	call_text:  string,
	call_defer: bool, // the slow-tool shape: Defer now, complete on the test's terms

	deferred_id:     jsonrpc.Id, // the call the host deferred (for the late completion)
	deferred_id_set: bool,
	cancelled:       bool,
	cancelled_id:    jsonrpc.Id,
	list_changed:    int,

	mu: sync.Mutex,
}

Cross_Box :: struct {
	host: ^Cross_Host,
}

cross_server_reader_entry :: proc(b: ^Cross_Box) {
	jsonrpc.conn_read_loop(b.host.server_conn)
}

cross_host_list :: proc(host: rawptr, arena: mem.Allocator) -> []mcp.Tool_Entry {
	h := cast(^Cross_Host)host
	return h.entries
}

cross_host_call :: proc(host: rawptr, call: ^mcp.Call_Info) -> mcp.Call_Outcome {
	h := cast(^Cross_Host)host
	for e in h.entries {
		if e.name == call.name {
			if h.call_defer {
				// The slow-tool shape: defer the answer and remember the
				// request so the test can complete (or abandon) it later.
				sync.mutex_lock(&h.mu)
				h.deferred_id = call.id
				h.deferred_id_set = call.id_set
				sync.mutex_unlock(&h.mu)
				return {deferred = true}
			}
			return {is_error = false, text = h.call_text}
		}
	}
	// Real hosts reject unknown tool names before execution; mirror that.
	return {is_protocol_error = true, err_code = .Invalid_Params, err_message = "unknown tool"}
}

cross_host_cancel :: proc(host: rawptr, id: jsonrpc.Id) {
	h := cast(^Cross_Host)host
	sync.mutex_lock(&h.mu)
	h.cancelled = true
	h.cancelled_id = id
	sync.mutex_unlock(&h.mu)
}

cross_client_list_changed :: proc(user: rawptr) {
	h := cast(^Cross_Host)user
	sync.mutex_lock(&h.mu)
	h.list_changed += 1
	sync.mutex_unlock(&h.mu)
}

// The transport fake: two one-way pipes behind the six verbs. close is
// the client's stdin side (the graceful half), kill closes the reply
// side (the peer's death, for the reader's EOF).
Cross_Pipes :: struct {
	req:  ^Pipe,
	resp: ^Pipe,
}

cross_transport_read :: proc(data: rawptr, buf: []u8) -> (int, jsonrpc.Read_Err) {
	p := cast(^Cross_Pipes)data
	return pipe_read(p.resp, buf)
}

cross_transport_write :: proc(data: rawptr, buf: []u8) -> (int, jsonrpc.Read_Err) {
	p := cast(^Cross_Pipes)data
	return pipe_write(p.req, buf)
}

cross_transport_close :: proc(data: rawptr) {
	p := cast(^Cross_Pipes)data
	pipe_close(p.req)
}

cross_transport_kill :: proc(data: rawptr) {
	p := cast(^Cross_Pipes)data
	pipe_close(p.resp)
}

cross_transport_probe :: proc(data: rawptr) -> bool {
	_ = data
	return false
}

cross_transport_destroy :: proc(data: rawptr) {
	_ = data
}

cross_setup :: proc(entries: []mcp.Tool_Entry) -> ^Cross_Host {
	req := new(Pipe, context.allocator)
	pipe_init(req)
	resp := new(Pipe, context.allocator)
	pipe_init(resp)

	h := new(Cross_Host, context.allocator)
	h^ = {
		req_pipe  = req,
		resp_pipe = resp,
		entries   = entries,
		call_text = "cross-ok",
	}

	// Server: NDJSON over the pipes (the stdio shape).
	sc := new(jsonrpc.Conn, context.allocator)
	sr: jsonrpc.Reader
	jsonrpc.reader_init_ndjson(&sr, pipe_read, req, 1024)
	sw: jsonrpc.Writer
	jsonrpc.writer_init_ndjson(&sw, pipe_write, resp)
	jsonrpc.conn_init(sc, sr, sw, context.allocator)

	s := new(mcp.Server, context.allocator)
	s^ = {
		host         = h,
		name         = "odin-mcp-cross",
		version      = "9.9",
		description  = "cross test server",
		instructions = "cross instructions",
		list_tools   = cross_host_list,
		call_tool    = cross_host_call,
		on_cancel    = cross_host_cancel,
	}
	h.server = s
	mcp.server_init(s, sc) // binds sc.host = s with the registrations
	h.server_conn = sc

	box := new(Cross_Box, context.allocator)
	box^ = {host = h}
	h.reader_box = box

	// Client: the wire core over the same pipes.
	pipes := new(Cross_Pipes, context.allocator)
	pipes^ = {req = req, resp = resp}
	c := new(mcp.Client, context.allocator)
	mcp.client_init(c, mcp.Transport{
		read_fn    = cross_transport_read,
		write_fn   = cross_transport_write,
		close_fn   = cross_transport_close,
		kill_fn    = cross_transport_kill,
		probe_fn   = cross_transport_probe,
		destroy_fn = cross_transport_destroy,
		data       = pipes,
	}, context.allocator)
	c.name = "cross-test"
	c.version = "1"
	c.on_list_changed = cross_client_list_changed
	c.hook_user = h
	h.client = c
	return h
}

cross_start :: proc(h: ^Cross_Host) -> bool {
	if mcp.client_start(h.client) != .None {
		return false
	}
	// The outbound writer is live from here, so the full client finish
	// path must run even when the reader thread fails to start.
	h.client_started = true
	if !mcp.client_start_reader(h.client) {
		return false
	}
	h.server_reader = thread.create_and_start_with_poly_data(h.reader_box, cross_server_reader_entry, self_cleanup = false)
	return h.server_reader != nil
}

cross_teardown :: proc(h: ^Cross_Host) {
	if h.client_started {
		jsonrpc.conn_close(&h.client.conn)
	}
	// The teardown ladder: the client's stdin side goes away (the
	// server's graceful half), then the reply side dies (the client
	// reader's EOF). Only then are the threads joined and the conns
	// freed — the same close-then-kill order the hosts are contractually
	// on.
	pipe_close(h.req_pipe)
	pipe_close(h.resp_pipe)
	if h.client_started {
		mcp.client_finish(h.client)
	}
	if h.server_reader != nil {
		thread.join(h.server_reader)
		free(h.server_reader, context.allocator)
	}
	free(h.reader_box, context.allocator)
	jsonrpc.conn_destroy(h.server_conn)
	free(h.server_conn, context.allocator)
	free(h.server, context.allocator)
	free(cast(^Cross_Pipes)h.client.transport.data, context.allocator)
	free(h.client, context.allocator)
	free(h.req_pipe, context.allocator)
	free(h.resp_pipe, context.allocator)
	free(h, context.allocator)
}

// --- version policies ---------------------------------------------------------

// The offered version crosses back from the server's reader thread, whose
// message arena dies right after dispatch and whose context allocator is
// not the test's: the handler clones into `allocator` (the test's own,
// carried in the struct) under the mutex, and the test takes the clone
// out and frees it.
Cross_Version_Answer :: struct {
	mu:        sync.Mutex,
	version:   string, // the fixed protocolVersion this fake answers
	seen:      string, // owned clone on `allocator` of what the client offered
	allocator: mem.Allocator,
}

cross_version_handler :: proc(conn: ^jsonrpc.Conn, env: ^jsonrpc.Envelope, arena: mem.Allocator) -> (jsonrpc.Reply, jsonrpc.Action) {
	st := cast(^Cross_Version_Answer)conn.host
	if v, ok := jsonutil.obj_get(env.params, "protocolVersion"); ok {
		#partial switch x in v {
		case json.String:
			sync.mutex_lock(&st.mu)
			if len(st.seen) > 0 {
				delete(st.seen, st.allocator)
			}
			st.seen = strings.clone(string(x), st.allocator)
			sync.mutex_unlock(&st.mu)
		case:
		}
	}
	result := jsonutil.json_object(1, arena)
	jsonutil.obj_set(&result, "protocolVersion", jsonutil.json_string(st.version))
	reply: jsonrpc.Reply = {result = obj_value(result)}
	return reply, .Respond
}

// A peer answering outside the table fails the handshake closed; a peer
// answering an older table entry negotiates down. The client offers
// PROTOCOL_LATEST, not the oldest entry — the specification's rule.
@(test)
cross_version_policies :: proc(t: ^testing.T) {
	{
		h := cross_setup(nil)
		defer cross_teardown(h)
		st := new(Cross_Version_Answer, context.allocator)
		st^ = {version = "1999-01-01", allocator = context.allocator}
		// The fake replaces the Server binding server_init installed; the
		// re-registered handler below reads it through conn.host. Both run
		// before cross_start — the server's reader thread must never race
		// the rebinding of conn.host or the handlers map.
		h.server_conn.host = st
		jsonrpc.conn_register(h.server_conn, "initialize", cross_version_handler)
		testing.expect(t, cross_start(h))
		msg, cerr := mcp.client_negotiate(h.client, context.temp_allocator)
		testing.expect_value(t, cerr, mcp.Client_Err.Unsupported_Version)
		testing.expect(t, len(msg) > 0, "the refusal carries prose")
		sync.mutex_lock(&st.mu)
		seen := st.seen
		st.seen = ""
		sync.mutex_unlock(&st.mu)
		testing.expect_value(t, seen, mcp.PROTOCOL_LATEST)
		delete(seen, context.allocator)
		free(st, context.allocator)
	}
	{
		h := cross_setup(nil)
		defer cross_teardown(h)
		st := new(Cross_Version_Answer, context.allocator)
		st^ = {version = "2024-11-05", allocator = context.allocator}
		// Same ordering as above: rebind and re-register before the reader
		// thread exists.
		h.server_conn.host = st
		jsonrpc.conn_register(h.server_conn, "initialize", cross_version_handler)
		testing.expect(t, cross_start(h))
		_, cerr := mcp.client_negotiate(h.client, context.temp_allocator)
		testing.expect_value(t, cerr, mcp.Client_Err.None)
		sync.mutex_lock(&st.mu)
		seen := st.seen
		st.seen = ""
		sync.mutex_unlock(&st.mu)
		testing.expect_value(t, seen, mcp.PROTOCOL_LATEST) // the offer the peer received
		delete(seen, context.allocator)
		testing.expect_value(t, h.client.protocol_version, "2024-11-05") // the negotiated downgrade
		free(st, context.allocator)
	}
}

// --- the full handshake and the tools round-trip -------------------------------

@(test)
cross_initialize_negotiates :: proc(t: ^testing.T) {
	arena: mem.Dynamic_Arena
	mem.dynamic_arena_init(&arena, context.allocator)
	defer mem.dynamic_arena_destroy(&arena)

	h := cross_setup(sample_entries(mem.dynamic_arena_allocator(&arena)))
	defer cross_teardown(h)
	testing.expect(t, cross_start(h))

	_, cerr := mcp.client_negotiate(h.client, mem.dynamic_arena_allocator(&arena))
	testing.expect_value(t, cerr, mcp.Client_Err.None)
	testing.expect_value(t, h.client.protocol_version, mcp.PROTOCOL_LATEST)

	// The initialized notification crossed and was served by the
	// server's reader thread (bounded poll — it races the reply).
	initialized := false
	for i := 0; i < 500; i += 1 {
		if mcp.server_is_initialized(h.server) {
			initialized = true
			break
		}
		time_sleep_ms(2)
	}
	testing.expect(t, initialized, "the initialized notification arrived")
}

@(test)
cross_tools_roundtrip :: proc(t: ^testing.T) {
	arena: mem.Dynamic_Arena
	mem.dynamic_arena_init(&arena, context.allocator)
	defer mem.dynamic_arena_destroy(&arena)

	h := cross_setup(sample_entries(mem.dynamic_arena_allocator(&arena)))
	defer cross_teardown(h)
	testing.expect(t, cross_start(h))
	_, cerr := mcp.client_negotiate(h.client, mem.dynamic_arena_allocator(&arena))
	testing.expect_value(t, cerr, mcp.Client_Err.None)

	tools: []mcp.Tool_Info
	msg:   string
	tools, msg, cerr = mcp.client_list_tools(h.client, mem.dynamic_arena_allocator(&arena))
	testing.expect_value(t, cerr, mcp.Client_Err.None)
	testing.expect_value(t, len(tools), 1)
	if len(tools) == 1 {
		testing.expect_value(t, tools[0].name, "sample_tool")
		testing.expect(t, tools[0].input_schema != nil, "the inputSchema passes through")
	}

	text:   string
	is_err: bool
	text, is_err, _, cerr = mcp.client_call_tool(h.client, "sample_tool", nil, mem.dynamic_arena_allocator(&arena), nil)
	testing.expect_value(t, cerr, mcp.Client_Err.None)
	testing.expect_value(t, is_err, false)
	testing.expect_value(t, text, "cross-ok")

	// An unknown tool surfaces as the server's protocol error, never as
	// a tool-level result.
	text, is_err, msg, cerr = mcp.client_call_tool(h.client, "nope", nil, mem.dynamic_arena_allocator(&arena), nil)
	testing.expect_value(t, cerr, mcp.Client_Err.Error_Response)
	testing.expect_value(t, is_err, true)
	testing.expect(t, strings.contains(msg, "unknown tool"), "the peer's message rides along, got %q", msg)
}

// --- cancellation ----------------------------------------------------------------

Cross_Call_Result :: struct {
	mu:     sync.Mutex,
	done:   bool,
	text:   string,
	is_err: bool,
	cerr:   mcp.Client_Err,
}

Cross_Call_Job :: struct {
	client: ^mcp.Client,
	name:   string,
	token:  ^platform.Cancel_Token,
	res:    ^Cross_Call_Result,
	a:      mem.Allocator, // clones the answer out of the call's arena
}

cross_caller_entry :: proc(j: ^Cross_Call_Job) {
	arena: mem.Dynamic_Arena
	mem.dynamic_arena_init(&arena, context.allocator)
	text, is_err, _, cerr := mcp.client_call_tool(j.client, j.name, nil, mem.dynamic_arena_allocator(&arena), j.token)
	// The answer borrows the arena — clone it out before the arena dies,
	// never after.
	cloned := strings.clone(text, j.a)
	mem.dynamic_arena_destroy(&arena)
	r := j.res
	sync.mutex_lock(&r.mu)
	r.text = cloned
	r.is_err = is_err
	r.cerr = cerr
	r.done = true
	sync.mutex_unlock(&r.mu)
}

@(test)
cross_cancellation_propagates :: proc(t: ^testing.T) {
	arena: mem.Dynamic_Arena
	mem.dynamic_arena_init(&arena, context.allocator)
	defer mem.dynamic_arena_destroy(&arena)

	slow := make([dynamic]mcp.Tool_Entry, 0, 1, mem.dynamic_arena_allocator(&arena))
	append(&slow, mcp.Tool_Entry{name = "slow", title = "Slow", description = "blocks"})
	h := cross_setup(slow[:])
	defer cross_teardown(h)
	h.call_defer = true
	testing.expect(t, cross_start(h))
	_, cerr := mcp.client_negotiate(h.client, mem.dynamic_arena_allocator(&arena))
	testing.expect_value(t, cerr, mcp.Client_Err.None)

	root := new(platform.Cancel_Token, context.allocator)
	platform.token_init_root(root)
	tok := platform.token_derive(root, 0, context.allocator)

	res := new(Cross_Call_Result, context.allocator)
	job := new(Cross_Call_Job, context.allocator)
	job^ = {client = h.client, name = "slow", token = tok, res = res, a = context.allocator}
	thr := thread.create_and_start_with_poly_data(job, cross_caller_entry, self_cleanup = false)

	deferred := false
	for i := 0; i < 500; i += 1 {
		sync.mutex_lock(&h.mu)
		deferred = h.deferred_id_set
		sync.mutex_unlock(&h.mu)
		if deferred {
			break
		}
		time_sleep_ms(2)
	}
	testing.expect(t, deferred, "the deferred call reached the host")
	platform.token_fire(tok, .Cancelled)

	thread.join(thr)
	free(thr, context.allocator)
	testing.expect_value(t, res.cerr, mcp.Client_Err.Cancelled)

	// The peer learned of the abandonment through notifications/cancelled,
	// with the client's numeric id intact.
	saw_cancel := false
	for i := 0; i < 500; i += 1 {
		sync.mutex_lock(&h.mu)
		c := h.cancelled
		sync.mutex_unlock(&h.mu)
		if c {
			saw_cancel = true
			break
		}
		time_sleep_ms(2)
	}
	testing.expect(t, saw_cancel, "the peer learned of the abandonment")
	sync.mutex_lock(&h.mu)
	id := h.cancelled_id
	sync.mutex_unlock(&h.mu)
	numeric := false
	#partial switch v in id {
	case i64:
		numeric = true
	case:
	}
	testing.expect(t, numeric, "the client's numeric id must stay numeric")

	// The host completes the abandoned call the way a real host would —
	// -32800 for the matched request. The client already moved on; the
	// late reply must be absorbed cleanly (the leak gate backs this).
	mcp.send_tool_error(h.server, id, true, .Request_Cancelled, "cancelled", context.temp_allocator)

	platform.token_destroy(tok, context.allocator)
	platform.token_destroy(root, context.allocator)
	free(job, context.allocator)
	free(res, context.allocator)
}

// --- list_changed -----------------------------------------------------------------

@(test)
cross_list_changed :: proc(t: ^testing.T) {
	h := cross_setup(nil)
	defer cross_teardown(h)
	testing.expect(t, cross_start(h))
	_, cerr := mcp.client_negotiate(h.client, context.temp_allocator)
	testing.expect_value(t, cerr, mcp.Client_Err.None)

	testing.expect(t, mcp.send_list_changed(h.server), "the notification crossed")
	seen := false
	for i := 0; i < 500; i += 1 {
		sync.mutex_lock(&h.mu)
		n := h.list_changed
		sync.mutex_unlock(&h.mu)
		if n >= 1 {
			seen = true
			break
		}
		time_sleep_ms(2)
	}
	testing.expect(t, seen, "the hook received tools/list_changed")
}

// --- raw frame contracts ------------------------------------------------------------

// Malformed JSON is answered with a Parse_Error carrying a null id, and
// the connection stops reading (the framing-rejection contract); a line
// over the peer's frame cap is a Too_Large death. Both drive the server
// conn by hand — no reader threads, every step deterministic.
@(test)
cross_raw_frame_contracts :: proc(t: ^testing.T) {
	h := cross_setup(nil)
	defer cross_teardown(h)

	bad := `{"jsonrpc":"2.0","id":1,"method":"ping","params":` // truncated body
	w: jsonrpc.Writer
	jsonrpc.writer_init_ndjson(&w, pipe_write, h.req_pipe)
	testing.expect(t, jsonrpc.write_frame(&w, json_bytes(bad)) == .None)

	frame, rerr := jsonrpc.read_frame(&h.server_conn.reader, context.temp_allocator)
	testing.expect(t, rerr == .None)
	keep := jsonrpc.conn_handle_body(h.server_conn, frame, context.temp_allocator)
	testing.expect(t, !keep, "a framing-level rejection must stop the read")

	// The rejection reply crossed to the response side (NDJSON framing —
	// the same shape the server writes).
	cr: jsonrpc.Reader
	jsonrpc.reader_init_ndjson(&cr, pipe_read, h.resp_pipe, 1024)
	defer jsonrpc.reader_destroy(&cr)
	reply, reply_err := jsonrpc.read_frame(&cr, context.temp_allocator)
	testing.expect(t, reply_err == .None)
	// The rejection reply carries a null id — JSON-RPC's rule for a body
	// that never decoded — so it is deliberately not decodable as a
	// response. Assert the shape on the wire bytes.
	testing.expect(t, strings.contains(string(reply), `"error"`), "the rejection answers an error object, got %s", string(reply))
	testing.expect(t, strings.contains(string(reply), "-32700"), "the rejection names Parse_Error")
	testing.expect(t, strings.contains(string(reply), `"id":null`), "the rejection carries a null id")

	// One line over the server's frame cap kills
	// the read with Too_Large — the body cap, not a reply.
	big := make([]u8, 2000, context.temp_allocator)
	for i in 0..<len(big) {
		big[i] = 'a'
	}
	testing.expect(t, jsonrpc.write_frame(&w, big) == .None)
	_, terr := jsonrpc.read_frame(&h.server_conn.reader, context.temp_allocator)
	testing.expect(t, terr == .Too_Large)
}
