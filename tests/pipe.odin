// The in-memory loopback pipe fake (writes feed the same pipe's reads):
// a transport-level pair for the suite's conns — real fd semantics
// (blocking read, close wakes readers) without processes.
package tests

import "core:mem"
import "core:sync"
import "core:time"
import "jsonrpc:jsonrpc"
import "jsonrpc:platform"

Pipe :: struct {
	buf:              [dynamic]u8,
	mu:               sync.Mutex,
	cond:             sync.Cond,
	closed:           bool,
	allocator:        mem.Allocator,
	// Optional per-read deadline in ms (0 = wait forever). A read that
	// would block longer returns .Eof instead of parking the reading
	// thread — for harnesses whose producer thread may die before writing
	// its frame, so the suite fails in bounded time instead of hanging.
	read_deadline_ms: i64,
}

pipe_init :: proc(p: ^Pipe, a := context.allocator) {
	p^ = {allocator = a}
}

// The pipe procs run on several threads (test, caller, reader); each pins
// the buffer to the pipe's own allocator so growth and free always match,
// whatever context the calling thread carries.
pipe_write :: proc(data: rawptr, buf: []u8) -> (int, jsonrpc.Read_Err) {
	p := cast(^Pipe)data
	context.allocator = p.allocator
	sync.mutex_lock(&p.mu)
	if p.closed {
		sync.mutex_unlock(&p.mu)
		return 0, .Eof
	}
	append(&p.buf, ..buf)
	sync.cond_broadcast(&p.cond)
	sync.mutex_unlock(&p.mu)
	return len(buf), .None
}

pipe_read :: proc(data: rawptr, buf: []u8) -> (int, jsonrpc.Read_Err) {
	p := cast(^Pipe)data
	context.allocator = p.allocator
	sync.mutex_lock(&p.mu)
	if p.read_deadline_ms > 0 {
		// Timed wait: cond_wait_with_timeout returns on the deadline even
		// without a signal, and the loop re-checks the buffer each wake.
		deadline := platform.mono_ms() + p.read_deadline_ms
		for len(p.buf) == 0 && !p.closed {
			remaining := deadline - platform.mono_ms()
			if remaining <= 0 {
				break
			}
			sync.cond_wait_with_timeout(&p.cond, &p.mu, time.Duration(remaining * 1_000_000))
		}
	} else {
		for len(p.buf) == 0 && !p.closed {
			sync.cond_wait(&p.cond, &p.mu)
		}
	}
	if len(p.buf) == 0 {
		sync.mutex_unlock(&p.mu)
		return 0, .Eof
	}
	n := min(len(p.buf), len(buf))
	for i := 0; i < n; i += 1 {
		buf[i] = p.buf[i]
	}
	rest := len(p.buf) - n
	for i := 0; i < rest; i += 1 {
		p.buf[i] = p.buf[i + n]
	}
	resize(&p.buf, rest)
	sync.mutex_unlock(&p.mu)
	return n, .None
}

pipe_close :: proc(p: ^Pipe) {
	context.allocator = p.allocator
	sync.mutex_lock(&p.mu)
	p.closed = true
	// Always delete: a fully drained buffer (len 0) still owns its backing.
	delete(p.buf)
	sync.cond_broadcast(&p.cond)
	sync.mutex_unlock(&p.mu)
}

// json_bytes copies a JSON literal into test scratch (backtick literals
// are untyped and cannot be transmuted directly).
json_bytes :: proc(s: string) -> []u8 {
	b := make([]u8, len(s), context.temp_allocator)
	for i in 0..<len(s) {
		b[i] = s[i]
	}
	return b
}

time_sleep_ms :: proc(ms: i64) {
	time.sleep(time.Duration(ms) * time.Millisecond)
}
