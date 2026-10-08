// mcp server tests with a fake client over two one-way pipes (client ->
// server requests, server -> client replies): initialize negotiation,
// ping (empty result, string-id echo), tools/list shape (icons [],
// _meta {}, annotations), tools/call outcomes (deferred worker send),
// the unknown-tool protocol error, and notifications/cancelled routing
// with ID type preservation.
package tests

import "core:encoding/json"
import "core:mem"
import "core:strings"
import "core:sync"
import "core:testing"
import "core:thread"
import "jsonrpc:jsonrpc"
import "jsonrpc:jsonutil"
import "mcp:mcp"
import "jsonrpc:platform"

// --- test harness -----------------------------------------------------------

Mcp_Host :: struct {
	server:       ^mcp.Server,
	server_conn:  ^jsonrpc.Conn,
	client_conn:  ^jsonrpc.Conn,
	req_pipe:     ^Pipe,
	resp_pipe:    ^Pipe,
	reader:       ^thread.Thread,
	reader_box:   ^Client_Box,

	entries:      []mcp.Tool_Entry,
	outcome:      mcp.Call_Outcome,
	cancelled_id: jsonrpc.Id,
	cancelled:    bool,
	notifications: [dynamic]string,
	custom_hit:   bool, // the server_register probe below ran

	caller_mu:  sync.Mutex,
	caller_done:   bool,
	caller_result: json.Value,
	caller_code:   jsonrpc.Err_Code,
	caller_msg:    string,
	caller_err:    jsonrpc.Call_Err,

	note_mu: sync.Mutex,
	allocator: mem.Allocator,
}

Client_Box :: struct {
	host: ^Mcp_Host,
}

mcp_client_reader_entry :: proc(b: ^Client_Box) {
	jsonrpc.conn_read_loop(b.host.client_conn)
}

mcp_host_list :: proc(host: rawptr, arena: mem.Allocator) -> []mcp.Tool_Entry {
	h := cast(^Mcp_Host)host
	return h.entries
}

mcp_host_call :: proc(host: rawptr, call: ^mcp.Call_Info) -> mcp.Call_Outcome {
	h := cast(^Mcp_Host)host
	for e in h.entries {
		if e.name == call.name {
			if h.outcome.deferred {
				// Simulate the worker completing a deferred call.
				mcp.send_tool_response(h.server, call.id, call.id_set, h.outcome.is_error, h.outcome.text, context.temp_allocator)
			}
			return h.outcome
		}
	}
	// Real hosts reject unknown tool names before execution; mirror that.
	return {
		is_protocol_error = true,
		err_code          = .Invalid_Params,
		err_message       = "unknown tool",
	}
}

mcp_host_cancel :: proc(host: rawptr, id: jsonrpc.Id) {
	h := cast(^Mcp_Host)host
	// Runs on the dispatch thread (the manual conn_handle_body below or a
	// reader thread in other harnesses): same lock discipline as every
	// other shared field of the host.
	sync.mutex_lock(&h.note_mu)
	h.cancelled_id = id
	h.cancelled = true
	sync.mutex_unlock(&h.note_mu)
}

mcp_note_recorder :: proc(conn: ^jsonrpc.Conn, env: ^jsonrpc.Envelope, arena: mem.Allocator) {
	h := cast(^Mcp_Host)conn.host
	// Runs on the client reader thread: pin to the host's own allocator and
	// clone — env.method dies with the message arena, and the thread's
	// context allocator differs from the test's.
	sync.mutex_lock(&h.note_mu)
	saved := context.allocator
	context.allocator = h.allocator
	append(&h.notifications, strings.clone(env.method, h.allocator))
	context.allocator = saved
	sync.mutex_unlock(&h.note_mu)
}

// The server_register probe: a method the built-ins do not cover, answered
// like any built-in. Reaches the harness through the Server face's host
// binding (conn.host -> Server -> Mcp_Host), not the client-side binding
// mcp_note_recorder uses.
custom_resources_handler :: proc(conn: ^jsonrpc.Conn, env: ^jsonrpc.Envelope, arena: mem.Allocator) -> (jsonrpc.Reply, jsonrpc.Action) {
	s := cast(^mcp.Server)conn.host
	h := cast(^Mcp_Host)s.host
	sync.mutex_lock(&h.note_mu)
	h.custom_hit = true
	sync.mutex_unlock(&h.note_mu)
	out := jsonutil.json_object(1, arena)
	jsonutil.obj_set(&out, "uri", jsonutil.json_string("mem://probe"))
	reply: jsonrpc.Reply = {result = obj_value(out)}
	return reply, .Respond
}

custom_note_handler :: proc(conn: ^jsonrpc.Conn, env: ^jsonrpc.Envelope, arena: mem.Allocator) {
	s := cast(^mcp.Server)conn.host
	h := cast(^Mcp_Host)s.host
	sync.mutex_lock(&h.note_mu)
	saved := context.allocator
	context.allocator = h.allocator
	append(&h.notifications, strings.clone(env.method, h.allocator))
	context.allocator = saved
	sync.mutex_unlock(&h.note_mu)
}

mcp_host_setup :: proc(entries: []mcp.Tool_Entry) -> ^Mcp_Host {
	req := new(Pipe, context.allocator)
	pipe_init(req)
	// Bound the main thread's request read: a caller thread that dies before
	// writing its frame would otherwise park read_frame forever and stall the
	// suite (the caller's own wait stays bounded by its conn_call deadline in
	// mcp_caller_entry). Both pipes carry a read deadline so a crash-or-timeout in
	// any role cannot wedge the runner.
	req.read_deadline_ms = 10_000
	resp := new(Pipe, context.allocator)
	pipe_init(resp)
	// Also bound the reply read: if the test crashes before teardown the
	// reader thread would otherwise park forever and wedge the runner.
	resp.read_deadline_ms = 5_000

	// Server: reads requests, writes replies.
	sc := new(jsonrpc.Conn, context.allocator)
	sr: jsonrpc.Reader
	jsonrpc.reader_init(&sr, pipe_read, req, 1024)
	sw: jsonrpc.Writer
	jsonrpc.writer_init(&sw, pipe_write, resp)
	jsonrpc.conn_init(sc, sr, sw, context.allocator)

	h := new(Mcp_Host, context.allocator)
	h^ = {
		server_conn = sc,
		client_conn = nil,
		req_pipe    = req,
		resp_pipe   = resp,
		entries     = entries,
		allocator    = context.allocator,
	}

	s := new(mcp.Server, context.allocator)
	s^ = {
		host         = h,
		name         = "odin-mcp-test",
		version      = "9.9",
		description  = "test server",
		instructions = "test instructions",
		list_tools   = mcp_host_list,
		call_tool    = mcp_host_call,
		on_cancel    = mcp_host_cancel,
	}
	h.server = s
	mcp.server_init(s, sc) // binds sc.host = s with the registrations

	// Client: writes requests into req, reads replies from resp.
	cc := new(jsonrpc.Conn, context.allocator)
	cr: jsonrpc.Reader
	jsonrpc.reader_init(&cr, pipe_read, resp, 1024)
	cw: jsonrpc.Writer
	jsonrpc.writer_init(&cw, pipe_write, req)
	jsonrpc.conn_init(cc, cr, cw, context.allocator)
	cc.host = h
	jsonrpc.conn_register_notification(cc, "notifications/tools/list_changed", mcp_note_recorder)
	h.client_conn = cc

	box := new(Client_Box, context.allocator)
	box^ = {host = h}
	h.reader_box = box
	h.reader = thread.create_and_start_with_poly_data(box, mcp_client_reader_entry, self_cleanup = false)
	return h
}

mcp_host_teardown :: proc(h: ^Mcp_Host) {
	jsonrpc.conn_close(h.client_conn)
	pipe_close(h.resp_pipe)
	jsonrpc.conn_close(h.server_conn)
	pipe_close(h.req_pipe)
	if h.reader != nil {
		thread.join(h.reader)
		free(h.reader, context.allocator)
	}
	free(h.reader_box, context.allocator)
	// Full conn teardown only after the reader thread is gone: conn_destroy
	// frees the reader state that thread was using.
	jsonrpc.conn_destroy(h.client_conn)
	jsonrpc.conn_destroy(h.server_conn)
	free(h.client_conn, context.allocator)
	free(h.server_conn, context.allocator)
	free(h.server, context.allocator)
	free(h.req_pipe, context.allocator)
	free(h.resp_pipe, context.allocator)
	sync.mutex_lock(&h.note_mu)
	for s in h.notifications {
		delete(s, h.allocator)
	}
	delete(h.notifications)
	sync.mutex_unlock(&h.note_mu)
	// Free the last caller result/error cloned into h.allocator by
	// conn_call via mcp_caller_entry — the test function is done with
	// it by the time teardown runs.
	jsonutil.free_value(h.caller_result, h.allocator)
	if len(h.caller_msg) > 0 {
		delete(h.caller_msg, h.allocator)
	}
	free(h, context.allocator)
}

Call_Box :: struct {
	host:   ^Mcp_Host,
	method: string,
	params: json.Value,
}

mcp_caller_entry :: proc(b: ^Call_Box) {
	h := b.host
	// Use context.temp_allocator for conn_call's arena: request body and
	// intermediate json values are short-lived and die with the thread.
	// Clone only the result and error message into h.allocator so they
	// outlive the caller thread without leaking the transient work.
	res, code, msg, err := jsonrpc.conn_call(h.client_conn, b.method, b.params, context.temp_allocator, platform.mono_ms() + 2000)
	sync.mutex_lock(&h.caller_mu)
	// Release any result the prior call left behind before overwriting:
	// the main thread finished reading it — mcp_do_call only returns
	// after joining this thread, and the next call starts after that.
	jsonutil.free_value(h.caller_result, h.allocator)
	if len(h.caller_msg) > 0 {
		delete(h.caller_msg, h.allocator)
	}
	h.caller_result = jsonutil.clone_value(res, h.allocator)
	h.caller_code = code
	h.caller_msg = strings.clone(msg, h.allocator)
	h.caller_err = err
	h.caller_done = true
	sync.mutex_unlock(&h.caller_mu)
}

// mcp_do_call runs the caller on a thread and serves exactly the one
// request frame it produces: the fake's replies are always written during
// dispatch (the simulated-deferred path included), so a single blocking
// read is the whole frame traffic of the call — no deadline poll loop.
mcp_do_call :: proc(h: ^Mcp_Host, method: string, params: json.Value) -> (json.Value, jsonrpc.Err_Code, string, jsonrpc.Call_Err) {
	sync.mutex_lock(&h.caller_mu)
	h.caller_done = false
	sync.mutex_unlock(&h.caller_mu)

	box := new(Call_Box, context.allocator)
	box^ = {host = h, method = method, params = params}
	thr := thread.create_and_start_with_poly_data(box, mcp_caller_entry, self_cleanup = false)

	body, rerr := jsonrpc.read_frame(&h.server_conn.reader, context.temp_allocator)
	if rerr == .None {
		a: mem.Dynamic_Arena
		mem.dynamic_arena_init(&a, context.allocator)
		jsonrpc.conn_handle_body(h.server_conn, body, mem.dynamic_arena_allocator(&a))
		mem.dynamic_arena_destroy(&a)
	}
	// If the frame never became readable the caller runs into its own
	// conn_call deadline set in mcp_caller_entry; the join is bounded either way.
	thread.join(thr)
	free(thr, context.allocator)
	free(box, context.allocator)

	sync.mutex_lock(&h.caller_mu)
	res := h.caller_result
	code := h.caller_code
	msg := h.caller_msg
	err := h.caller_err
	sync.mutex_unlock(&h.caller_mu)
	return res, code, msg, err
}

// --- fixtures ----------------------------------------------------------------

sample_entries :: proc(arena: mem.Allocator) -> []mcp.Tool_Entry {
	entries := make([dynamic]mcp.Tool_Entry, arena)
	schema := jsonutil.json_object(2, arena)
	jsonutil.obj_set(&schema, "type", jsonutil.json_string("object"))
	entry := mcp.Tool_Entry{
		name             = "sample_tool",
		title            = "Sample",
		description      = "A sample.",
		input_schema     = obj_value(schema),
		read_only_hint   = true,
		destructive_hint = false,
	}
	append(&entries, entry)
	return entries[:]
}

obj_has :: proc(v: json.Value, key: string) -> bool {
	_, ok := jsonutil.obj_get(v, key)
	return ok
}

obj_value :: proc(m: map[string]json.Value) -> json.Value {
	return json.Value(json.Object(m))
}

obj_str :: proc(v: json.Value, key: string) -> string {
	if val, ok := jsonutil.obj_get(v, key); ok {
		#partial switch x in val {
		case json.String:
			return string(x)
		case:
		}
	}
	return ""
}

// --- tests ---------------------------------------------------------------------

@(test)
mcp_initialize_negotiation :: proc(t: ^testing.T) {
	arena: mem.Dynamic_Arena
	mem.dynamic_arena_init(&arena, context.allocator)
	defer mem.dynamic_arena_destroy(&arena)

	h := mcp_host_setup(sample_entries(mem.dynamic_arena_allocator(&arena)))
	defer mcp_host_teardown(h)

	params := jsonutil.json_object(1, context.temp_allocator)
	jsonutil.obj_set(&params, "protocolVersion", jsonutil.json_string("2025-06-18"))
	result, _, _, cerr := mcp_do_call(h, "initialize", obj_value(params))
	testing.expect_value(t, cerr, jsonrpc.Call_Err.None)
	testing.expect_value(t, obj_str(result, "protocolVersion"), "2025-06-18")

	params2 := jsonutil.json_object(1, context.temp_allocator)
	jsonutil.obj_set(&params2, "protocolVersion", jsonutil.json_string("1999-01-01"))
	result, _, _, cerr = mcp_do_call(h, "initialize", obj_value(params2))
	testing.expect_value(t, cerr, jsonrpc.Call_Err.None)
	testing.expect_value(t, obj_str(result, "protocolVersion"), mcp.PROTOCOL_LATEST)

	testing.expect(t, obj_has(result, "instructions"))
	testing.expect(t, obj_has(result, "capabilities"))
	testing.expect(t, obj_has(result, "serverInfo"))
}

// Emptiness predicates for the shape-fixed tools/list fields (icons [],
// _meta {}); local to this test — nothing in src needs them.
is_empty_array :: proc(v: json.Value) -> bool {
	arr, ok := jsonutil.as_array(v)
	return ok && len(arr) == 0
}

is_empty_object :: proc(v: json.Value) -> bool {
	m, ok := jsonutil.as_object(v)
	return ok && len(m) == 0
}

@(test)
mcp_tools_list_shape :: proc(t: ^testing.T) {
	arena: mem.Dynamic_Arena
	mem.dynamic_arena_init(&arena, context.allocator)
	defer mem.dynamic_arena_destroy(&arena)

	h := mcp_host_setup(sample_entries(mem.dynamic_arena_allocator(&arena)))
	defer mcp_host_teardown(h)

	result, _, _, cerr := mcp_do_call(h, "tools/list", nil)
	testing.expect_value(t, cerr, jsonrpc.Call_Err.None)
	tools_val, ok := jsonutil.obj_get(result, "tools")
	testing.expect(t, ok, "tools array missing")
	arr, is_arr := jsonutil.as_array(tools_val)
	testing.expect(t, is_arr && len(arr) == 1)
	first := arr[0]

	testing.expect(t, obj_has(first, "name"))
	testing.expect(t, obj_has(first, "title"))
	testing.expect(t, obj_has(first, "annotations"))
	testing.expect(t, obj_has(first, "inputSchema"))

	// Shape-fixed fields: icons always [], _meta always {}.
	icons, icons_ok := jsonutil.obj_get(first, "icons")
	testing.expect(t, icons_ok && is_empty_array(icons), "icons must be []")
	meta, meta_ok := jsonutil.obj_get(first, "_meta")
	testing.expect(t, meta_ok && is_empty_object(meta), "_meta must be {}")

	annotations, _ := jsonutil.obj_get(first, "annotations")
	ro := jsonutil.obj_get_bool(annotations, "readOnlyHint")
	testing.expect_value(t, ro, true)
}

@(test)
mcp_tools_call_outcomes :: proc(t: ^testing.T) {
	arena: mem.Dynamic_Arena
	mem.dynamic_arena_init(&arena, context.allocator)
	defer mem.dynamic_arena_destroy(&arena)

	h := mcp_host_setup(sample_entries(mem.dynamic_arena_allocator(&arena)))
	defer mcp_host_teardown(h)

	// Deferred outcome completed by the fake worker inside call_tool.
	h.outcome = {deferred = true, is_error = false, text = "done!"}
	params := jsonutil.json_object(2, context.temp_allocator)
	jsonutil.obj_set(&params, "name", jsonutil.json_string("sample_tool"))
	jsonutil.obj_set_object(&params, "arguments", jsonutil.json_object(0, context.temp_allocator))
	result, _, _, cerr := mcp_do_call(h, "tools/call", obj_value(params))
	testing.expect_value(t, cerr, jsonrpc.Call_Err.None)
	testing.expect_value(t, jsonutil.obj_get_bool(result, "isError"), false)

	// Immediate outcome: the server answers inline. The reply must be valid
	// JSON carrying the request id — a body without the id (or truncated)
	// never matches the pending slot and the call times out instead.
	h.outcome = {deferred = false, is_error = true, text = "boom"}
	result, _, _, cerr = mcp_do_call(h, "tools/call", obj_value(params))
	testing.expect_value(t, cerr, jsonrpc.Call_Err.None)
	testing.expect_value(t, jsonutil.obj_get_bool(result, "isError"), true)

	// Unknown tool: protocol error -32602, not an isError result.
	jsonutil.obj_set(&params, "name", jsonutil.json_string("nope"))
	_, nf_code, _, nf_cerr := mcp_do_call(h, "tools/call", obj_value(params))
	testing.expect_value(t, nf_cerr, jsonrpc.Call_Err.Error_Response)
	testing.expect_value(t, nf_code, jsonrpc.Err_Code.Invalid_Params)

	// arguments, when present, must be an object — a string is a protocol
	// error, never a tool-level validation failure.
	jsonutil.obj_set(&params, "name", jsonutil.json_string("sample_tool"))
	jsonutil.obj_set(&params, "arguments", jsonutil.json_string("not-an-object"))
	_, ao_code, _, ao_cerr := mcp_do_call(h, "tools/call", obj_value(params))
	testing.expect_value(t, ao_cerr, jsonrpc.Call_Err.Error_Response)
	testing.expect_value(t, ao_code, jsonrpc.Err_Code.Invalid_Params)

	// notifications/cancelled routes the id to the host with its JSON type
	// intact. The notify was fully written before conn_notify returned, so
	// exactly one frame is pending: a single blocking read serves it
	// deterministically.
	cancel := jsonutil.json_object(1, context.temp_allocator)
	jsonutil.obj_set(&cancel, "requestId", jsonutil.json_string("42"))
	testing.expect(t, jsonrpc.conn_notify(h.client_conn, "notifications/cancelled", obj_value(cancel), context.temp_allocator))
	frame, frame_err := jsonrpc.read_frame(&h.server_conn.reader, context.temp_allocator)
	testing.expect(t, frame_err == .None, "cancel frame must be readable")
	if frame_err == .None {
		a: mem.Dynamic_Arena
		mem.dynamic_arena_init(&a, context.allocator)
		jsonrpc.conn_handle_body(h.server_conn, frame, mem.dynamic_arena_allocator(&a))
		mem.dynamic_arena_destroy(&a)
	}
	sync.mutex_lock(&h.note_mu)
	cancelled := h.cancelled
	cancelled_id := h.cancelled_id
	sync.mutex_unlock(&h.note_mu)
	testing.expect(t, cancelled, "cancel notification not delivered")
#partial switch v in cancelled_id {
	case string:
		testing.expect_value(t, v, "42") // string ids keep their type
	case:
		testing.expectf(t, false, "cancelled id must stay a string")
	}
}

@(test)
mcp_ping_answered :: proc(t: ^testing.T) {
	arena: mem.Dynamic_Arena
	mem.dynamic_arena_init(&arena, context.allocator)
	defer mem.dynamic_arena_destroy(&arena)

	h := mcp_host_setup(sample_entries(mem.dynamic_arena_allocator(&arena)))
	defer mcp_host_teardown(h)

	// The liveness probe answers an empty result, never -32601.
	result, _, _, cerr := mcp_do_call(h, "ping", nil)
	testing.expect_value(t, cerr, jsonrpc.Call_Err.None)
	empty, is_obj := jsonutil.as_object(result)
	testing.expect(t, is_obj && len(empty) == 0, "ping result must be an empty object")
}

// A request whose id is the STRING "42" must be answered with the string —
// JSON-RPC requires the reply to echo the same value. This drives the raw
// frame path (no client conn involved): the reply is read straight from the
// response pipe.
@(test)
mcp_string_id_echoed :: proc(t: ^testing.T) {
	req := new(Pipe, context.allocator)
	pipe_init(req)
	resp := new(Pipe, context.allocator)
	pipe_init(resp)

	sc := new(jsonrpc.Conn, context.allocator)
	sr: jsonrpc.Reader
	jsonrpc.reader_init(&sr, pipe_read, req, 1024)
	sw: jsonrpc.Writer
	jsonrpc.writer_init(&sw, pipe_write, resp)
	jsonrpc.conn_init(sc, sr, sw, context.allocator)

	s := new(mcp.Server, context.allocator)
	s^ = {
		list_tools = mcp_host_list,
		call_tool  = mcp_host_call,
		on_cancel  = mcp_host_cancel,
	}
	mcp.server_init(s, sc)

	qw: jsonrpc.Writer
	jsonrpc.writer_init(&qw, pipe_write, req)
	body := `{"jsonrpc":"2.0","id":"42","method":"ping"}`
	testing.expect(t, jsonrpc.write_frame(&qw, transmute([]u8)body) == .None)
	// Read through the conn's own reader (conn_init copies the struct — a
	// stack-side read would grow a buffer nobody destroys).
	frame, rerr := jsonrpc.read_frame(&sc.reader, context.temp_allocator)
	testing.expect(t, rerr == .None)
	a: mem.Dynamic_Arena
	mem.dynamic_arena_init(&a, context.allocator)
	jsonrpc.conn_handle_body(sc, frame, mem.dynamic_arena_allocator(&a))
	mem.dynamic_arena_destroy(&a)

	cr: jsonrpc.Reader
	jsonrpc.reader_init(&cr, pipe_read, resp, 1024)
	defer jsonrpc.reader_destroy(&cr)
	reply, rerr2 := jsonrpc.read_frame(&cr, context.temp_allocator)
	testing.expect(t, rerr2 == .None)
	testing.expect(t, strings.contains(string(reply), `"id":"42"`), "string id must be echoed as a string, got: %s", string(reply))
	testing.expect(t, strings.contains(string(reply), `"result":{}`), "ping reply must carry an empty result")

	pipe_close(resp)
	pipe_close(req)
	jsonrpc.conn_destroy(sc)
	free(sc, context.allocator)
	free(s, context.allocator)
	free(req, context.allocator)
	free(resp, context.allocator)
}

// The extension surface README documents: methods and notifications the
// built-ins do not cover register through the Server face and behave like
// built-ins on the same conn.
@(test)
mcp_server_register_extension_surface :: proc(t: ^testing.T) {
	arena: mem.Dynamic_Arena
	mem.dynamic_arena_init(&arena, context.allocator)
	defer mem.dynamic_arena_destroy(&arena)

	h := mcp_host_setup(nil)
	defer mcp_host_teardown(h)

	mcp.server_register(h.server, "resources/list", custom_resources_handler)
	mcp.server_register_notification(h.server, "notifications/custom", custom_note_handler)

	result, _, _, cerr := mcp_do_call(h, "resources/list", nil)
	testing.expect_value(t, cerr, jsonrpc.Call_Err.None)
	testing.expect_value(t, obj_str(result, "uri"), "mem://probe")
	sync.mutex_lock(&h.note_mu)
	hit := h.custom_hit
	sync.mutex_unlock(&h.note_mu)
	testing.expect(t, hit, "the registered handler ran on the server conn")

	// The registered notification routes through the server's notifier
	// table with the same single-frame determinism the cancelled test
	// relies on: conn_notify fully wrote before returning.
	testing.expect(t, jsonrpc.conn_notify(h.client_conn, "notifications/custom", nil, context.temp_allocator))
	frame, frame_err := jsonrpc.read_frame(&h.server_conn.reader, context.temp_allocator)
	testing.expect(t, frame_err == .None, "custom notification frame must be readable")
	if frame_err == .None {
		a: mem.Dynamic_Arena
		mem.dynamic_arena_init(&a, context.allocator)
		jsonrpc.conn_handle_body(h.server_conn, frame, mem.dynamic_arena_allocator(&a))
		mem.dynamic_arena_destroy(&a)
	}
	sync.mutex_lock(&h.note_mu)
	seen := len(h.notifications)
	sync.mutex_unlock(&h.note_mu)
	testing.expect_value(t, seen, 1)
}
