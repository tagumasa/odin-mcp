// mcp: the MCP client wire core (protocol ladder through 2025-11-25).
// Transport-agnostic: the host fills a Transport (six verbs over any
// byte stream — a spawned child's pipes are the production form) and
// drives its lifecycle: close/kill/probe/destroy, process spawning,
// reconnect backoff, registries, and secret injection stay host-side.
// The wire half is here: NDJSON conn wiring with the mandatory outbound
// writer, initialize negotiation against the shared version table,
// tools/list and tools/call with cancellation, and a generic call
// surface every other v1-ladder method rides.
package mcp

import "core:encoding/json"
import "core:fmt"
import "core:mem"
import "core:strings"
import "core:thread"

import "jsonrpc:jsonrpc"
import "jsonrpc:platform"
import "jsonutil:jsonutil"

CLIENT_INIT_TIMEOUT_MS :: i64(10_000)
CLIENT_CALL_TIMEOUT_MS :: i64(30_000)
CLIENT_MAX_FRAME_BYTES :: 4 << 20

// Client_Err is the closed client-face outcome vocabulary: conn_call's
// client-role outcomes plus the two shape/negotiation failures. The
// accompanying message is best-effort prose; callers branch on the
// Client_Err, never on the text.
Client_Err :: enum {
	None,
	Timeout,
	Cancelled,
	Closed,
	Transport,
	Error_Response,     // the peer answered with a JSON-RPC error object
	Unsupported_Version, // the peer answered outside the version table
	Bad_Shape,           // the peer's result lacked the shape the method requires
	Internal,
}

// Transport abstracts the peer's byte stream. The production form holds
// a spawned child's pipes; hosts inject anything else (an in-memory
// channel pair, a socket) through the same verbs. close/kill/probe/
// destroy are the host's teardown ladder — the wire core only reads
// and writes.
Transport :: struct {
	read_fn:  proc(data: rawptr, buf: []u8) -> (int, jsonrpc.Read_Err),
	write_fn: proc(data: rawptr, buf: []u8) -> (int, jsonrpc.Read_Err),
	// close_fn shuts the client side of the child's stdin — the graceful
	// half of teardown.
	close_fn: proc(data: rawptr),
	// kill_fn is the process-group kill.
	kill_fn:  proc(data: rawptr),
	// probe_fn reports whether the peer has exited.
	probe_fn: proc(data: rawptr) -> bool,
	// destroy_fn frees the transport's own state (post-join).
	destroy_fn: proc(data: rawptr),
	data: rawptr,
}

// List_Changed_Hook receives the peer's notifications/tools/list_changed
// on the client's reader thread. The re-fold policy is the host's — hold
// it for a turn boundary, refresh eagerly, whatever the host chooses.
List_Changed_Hook :: proc(user: rawptr)

// Dead_Hook fires on the reader thread when the read loop exits: the
// wire is gone (the peer died or the host tore the transport down).
Dead_Hook :: proc(user: rawptr)

Client :: struct {
	conn:      jsonrpc.Conn,
	transport: Transport, // host-filled before client_start

	name:    string, // clientInfo name (borrowed; the host owns its storage)
	version: string, // clientInfo version (borrowed)

	call_timeout_ms:  i64,    // tools/call deadline budget
	protocol_version: string, // the negotiated revision; owned clone, set by client_negotiate

	on_list_changed: List_Changed_Hook, // nil = ignored
	on_dead:         Dead_Hook,         // nil = ignored
	hook_user:       rawptr,

	reader_th: ^thread.Thread,
	allocator: mem.Allocator,
}

// client_init binds a transport. The Client must stay at one address
// afterwards: the conn's adapters keep a pointer to the transport
// inside it.
client_init :: proc(c: ^Client, t: Transport, a := context.allocator) {
	c^ = {
		transport       = t,
		call_timeout_ms = CLIENT_CALL_TIMEOUT_MS,
		allocator       = a,
	}
}

// client_start wires the NDJSON conn over the transport and installs
// the outbound writer. The writer is mandatory: a peer that stops
// reading stdin must block exactly its own writer thread, never a
// calling turn.
client_start :: proc(c: ^Client) -> Client_Err {
	reader := jsonrpc.Reader{}
	jsonrpc.reader_init_ndjson(&reader, transport_read_adapter, &c.transport, CLIENT_MAX_FRAME_BYTES, c.allocator)
	writer := jsonrpc.Writer{}
	jsonrpc.writer_init_ndjson(&writer, transport_write_adapter, &c.transport)
	jsonrpc.conn_init(&c.conn, reader, writer, c.allocator)
	c.conn.host = c
	c.conn.cancel_notify = client_cancelled_notify
	jsonrpc.conn_register_notification(&c.conn, "notifications/tools/list_changed", client_list_changed_notify)
	if !jsonrpc.conn_start_outbound(&c.conn, jsonrpc.OUTBOUND_FRAMES_CAP, jsonrpc.OUTBOUND_BYTES_CAP) {
		return .Internal
	}
	return .None
}

// client_start_reader runs the read loop on its own thread; its exit
// marks the wire dead (the on_dead hook fires on the reader thread).
client_start_reader :: proc(c: ^Client) -> bool {
	// The core allocates the ^Thread handle from the caller's ambient
	// context.allocator while client_finish's join frees it through
	// c.allocator: pin the ambient for the spawn so both sides name the
	// same owner.
	thread_alloc := context.allocator
	context.allocator = c.allocator
	c.reader_th = thread.create_and_start_with_poly_data(c, client_reader_entry, self_cleanup = false, name = "mcp-client-reader")
	context.allocator = thread_alloc
	return c.reader_th != nil
}

client_reader_entry :: proc(c: ^Client) {
	jsonrpc.conn_read_loop(&c.conn)
	if c.on_dead != nil {
		c.on_dead(c.hook_user)
	}
}

// client_close marks the connection closed and releases every waiter.
// The transport's teardown ladder (stdin EOF, bounded grace, kill) is
// the host's policy and runs in the host's order.
client_close :: proc(c: ^Client) {
	jsonrpc.conn_close(&c.conn)
}

// client_finish tears the client's own state down: join the reader
// (call only once the wire is dead — the transport ladder must have run
// first, or the parked read never returns), destroy the conn, release
// owned strings.
client_finish :: proc(c: ^Client) {
	client_join_reader(c)
	jsonrpc.conn_destroy(&c.conn)
	if len(c.protocol_version) > 0 {
		delete(c.protocol_version, c.allocator)
		c.protocol_version = ""
	}
}

client_join_reader :: proc(c: ^Client) {
	if c.reader_th == nil {
		return
	}
	thread.join(c.reader_th)
	free(c.reader_th, c.allocator)
	c.reader_th = nil
}

// ---------------------------------------------------------------------------
// The generic request surface and the tools conveniences.
// ---------------------------------------------------------------------------

// client_call is the generic request surface: every v1-ladder method the
// conveniences below do not cover rides this one (resources, prompts,
// logging, completion). params may be nil; the result allocates on
// `arena`. deadline_ms is an absolute monotonic timestamp (0 = none).
client_call :: proc(c: ^Client, method: string, params: json.Value, arena: mem.Allocator, deadline_ms: i64, token: ^platform.Cancel_Token = nil) -> (result: json.Value, msg: string, cerr: Client_Err) {
	res, _, peer_msg, call_err := jsonrpc.conn_call(&c.conn, method, params, arena, deadline_ms, token)
	cerr, msg = client_error_of(call_err, peer_msg)
	return res, msg, cerr
}

// client_error_of folds conn_call's outcome into the client vocabulary.
// The switch is partial on purpose: the fold runs against whichever
// jsonrpc.Call_Err the embedding host carries, and any member outside
// the mapped set answers the fallthrough's internal-error refusal
// rather than failing to compile against a richer vocabulary.
client_error_of :: proc(call_err: jsonrpc.Call_Err, peer_msg: string) -> (cerr: Client_Err, msg: string) {
	#partial switch call_err {
	case .None:           return .None, ""
	case .Timeout:        return .Timeout, "the server did not answer inside the deadline"
	case .Cancelled:      return .Cancelled, "cancelled"
	case .Closed:         return .Closed, "connection closed"
	case .Transport:      return .Transport, "transport failure"
	case .Error_Response: return .Error_Response, peer_msg
	}
	return .Internal, "internal error"
}

// client_negotiate runs initialize — it offers PROTOCOL_LATEST, accepts
// any table entry, and fails the connection outside the table — then
// sends the initialized notification. On success c.protocol_version
// carries the answered revision.
client_negotiate :: proc(c: ^Client, arena: mem.Allocator) -> (msg: string, cerr: Client_Err) {
	root := jsonutil.json_object(3, arena)
	jsonutil.obj_set(&root, "protocolVersion", jsonutil.json_string(PROTOCOL_LATEST))
	caps := jsonutil.json_object(0, arena)
	jsonutil.obj_set_object(&root, "capabilities", caps)
	info := jsonutil.json_object(2, arena)
	jsonutil.obj_set(&info, "name", jsonutil.json_string(c.name))
	jsonutil.obj_set(&info, "version", jsonutil.json_string(c.version))
	jsonutil.obj_set_object(&root, "clientInfo", info)

	result: json.Value
	result, msg, cerr = client_call(c, "initialize", object_value(root), arena, platform.mono_ms() + CLIENT_INIT_TIMEOUT_MS)
	if cerr != .None {
		return msg, cerr
	}

	answered := ""
	if root_obj, ok := jsonutil.as_object(result); ok {
		if v, has := root_obj["protocolVersion"]; has {
			#partial switch sv in v {
			case json.String:
				answered = sv
			}
		}
	}
	if !protocol_version_supported(answered) {
		return fmt.aprintf(
			"the server answered protocol version %s, which this client does not speak",
			answered, allocator = arena,
		), .Unsupported_Version
	}
	if len(c.protocol_version) > 0 {
		delete(c.protocol_version, c.allocator)
	}
	c.protocol_version = strings.clone(answered, c.allocator)

	jsonrpc.conn_notify(&c.conn, "notifications/initialized", nil, arena)
	return "", .None
}

// Tool_Info is one entry of a tools/list answer. The strings are clones
// on the call's arena; input_schema is the pass-through value (nil when
// the tool declared none). Copy out what outlives the arena.
Tool_Info :: struct {
	name:         string,
	description:  string,
	input_schema: json.Value,
}

// client_list_tools pulls tools/list (one page). The first-page request
// carries no params at all: the cursor member is optional-object on the
// wire, and an empty-string cursor is not its canonical absence — hosts
// paginate through client_call with the answered nextCursor. The entries
// allocate on `arena`.
client_list_tools :: proc(c: ^Client, arena: mem.Allocator) -> (tools: []Tool_Info, msg: string, cerr: Client_Err) {
	result: json.Value
	result, msg, cerr = client_call(c, "tools/list", nil, arena, platform.mono_ms() + CLIENT_INIT_TIMEOUT_MS)
	if cerr != .None {
		return nil, msg, cerr
	}
	root_obj, ok := jsonutil.as_object(result)
	if !ok {
		return nil, "tools/list answered without an object", .Bad_Shape
	}
	tools_v, has_tools := root_obj["tools"]
	if !has_tools {
		return nil, "tools/list answered without a tools array", .Bad_Shape
	}
	arr, arr_ok := jsonutil.as_array(tools_v)
	if !arr_ok {
		return nil, "tools/list answered without a tools array", .Bad_Shape
	}

	out := make([dynamic]Tool_Info, 0, len(arr), arena)
	for tv in arr {
		tobj, tok := jsonutil.as_object(tv)
		if !tok {
			continue
		}
		name_v, has_name := tobj["name"]
		if !has_name {
			continue
		}
		tool_name := ""
		#partial switch nv in name_v {
		case json.String:
			tool_name = nv
		}
		if tool_name == "" {
			continue
		}
		description := ""
		if dv, has_d := tobj["description"]; has_d {
			#partial switch d in dv {
			case json.String:
				description = d
			}
		}
		schema: json.Value = nil
		if sv, has_s := tobj["inputSchema"]; has_s && sv != nil {
			schema = sv
		}
		append(&out, Tool_Info{
			name         = strings.clone(tool_name, arena),
			description  = strings.clone(description, arena),
			input_schema = schema,
		})
	}
	return out[:], "", .None
}

// client_call_tool runs one tools/call and flattens the result: text
// contents joined with newlines, non-text noted, isError flagged.
// arguments may be nil (an empty arguments object goes on the wire).
// The deadline is the client's call_timeout_ms; cancellation propagates
// through the token, and the peer is told through
// notifications/cancelled by the conn's hook.
client_call_tool :: proc(c: ^Client, name: string, arguments: json.Value, arena: mem.Allocator, token: ^platform.Cancel_Token = nil) -> (text: string, is_error: bool, msg: string, cerr: Client_Err) {
	params := jsonutil.json_object(2, arena)
	jsonutil.obj_set(&params, "name", jsonutil.json_string(name))
	args: json.Value = json.Value(json.Object(jsonutil.json_object(0, arena)))
	if arguments != nil {
		args = arguments
	}
	jsonutil.obj_set(&params, "arguments", args)

	result: json.Value
	result, msg, cerr = client_call(c, "tools/call", object_value(params), arena, platform.mono_ms() + c.call_timeout_ms, token)
	if cerr != .None {
		return "", true, msg, cerr
	}
	text, is_error, msg, cerr = client_render_result(result, name, arena)
	return text, is_error, msg, cerr
}

// client_render_result flattens a tools/call result the host already
// holds (the render half of client_call_tool).
client_render_result :: proc(result: json.Value, name: string, a: mem.Allocator) -> (text: string, is_error: bool, msg: string, cerr: Client_Err) {
	root_obj, ok := jsonutil.as_object(result)
	if !ok {
		return "", true, "the server returned a malformed result", .Bad_Shape
	}
	if v, has_is := root_obj["isError"]; has_is {
		if bb, is_bool := v.(json.Boolean); is_bool {
			is_error = bool(bb)
		}
	}
	parts := make([dynamic]string, 0, 4, a)
	defer delete(parts) // frees through the allocator the array stored at make
	if cv, has_content := root_obj["content"]; has_content {
		if arr, arr_ok := jsonutil.as_array(cv); arr_ok {
			for item in arr {
				iobj, iok := jsonutil.as_object(item)
				if !iok {
					continue
				}
				kind := "text"
				if kv, has_type := iobj["type"]; has_type {
					#partial switch k in kv {
					case json.String:
						kind = k
					}
				}
				if kind == "text" {
					if tv, has_text := iobj["text"]; has_text {
						#partial switch tx in tv {
						case json.String:
							append(&parts, tx)
						}
					}
				} else {
					append(&parts, fmt.aprintf("[%s content omitted]", kind, allocator = a))
				}
			}
		}
	}
	if len(parts) == 0 {
		append(&parts, fmt.aprintf("%s returned no content", name, allocator = a))
	}
	// The Client_Err carries no claim on success — the closed vocabulary
	// has no success member, and the caller gates on is_error alone (the
	// same hint-only shape conn_call documents for its error code).
	return strings.join(parts[:], "\n", a), is_error, "", .None
}

// ---------------------------------------------------------------------------
// The conn's notification hooks and the transport adapters.
// ---------------------------------------------------------------------------

// client_cancelled_notify tells the peer a call was abandoned locally —
// the hook the conn fires when a caller's token fired first.
client_cancelled_notify :: proc(conn: ^jsonrpc.Conn, id: i64) {
	arena: mem.Dynamic_Arena
	mem.dynamic_arena_init(&arena, conn.allocator, block_size = jsonrpc.MESSAGE_ARENA_BLOCK_SIZE)
	defer mem.dynamic_arena_destroy(&arena)
	a := mem.dynamic_arena_allocator(&arena)
	params := jsonutil.json_object(1, a)
	jsonutil.obj_set(&params, "requestId", jsonutil.json_int(id))
	jsonrpc.conn_notify(conn, "notifications/cancelled", object_value(params), a)
}

// client_list_changed_notify receives the peer's tools/list_changed and
// hands it to the host hook; without one the notification is dropped.
client_list_changed_notify :: proc(conn: ^jsonrpc.Conn, env: ^jsonrpc.Envelope, arena: mem.Allocator) {
	_ = env
	_ = arena
	c := cast(^Client)conn.host
	if c == nil || c.on_list_changed == nil {
		return
	}
	c.on_list_changed(c.hook_user)
}

transport_read_adapter :: proc(data: rawptr, buf: []u8) -> (int, jsonrpc.Read_Err) {
	t := cast(^Transport)data
	return t.read_fn(t.data, buf)
}

transport_write_adapter :: proc(data: rawptr, buf: []u8) -> (int, jsonrpc.Read_Err) {
	t := cast(^Transport)data
	return t.write_fn(t.data, buf)
}

object_value :: proc(m: map[string]json.Value) -> json.Value {
	return json.Value(json.Object(m))
}
