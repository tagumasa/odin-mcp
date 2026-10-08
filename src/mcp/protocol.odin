// The protocol-version table: the single constant both faces reference,
// with the two mismatch policies that hang off it.
package mcp

PROTOCOL_LATEST :: "2025-11-25"

// The stateful ladder this SDK speaks. The client offers
// PROTOCOL_LATEST (the specification's rule: send the newest you
// support) and accepts any table entry, failing the connection outside
// it; the server answers the requested entry when it is in the table
// and PROTOCOL_LATEST otherwise.
PROTOCOL_SUPPORTED :: []string{"2024-11-05", "2025-03-26", "2025-06-18", "2025-11-25"}

// protocol_version_supported reports whether v is a table entry. The
// constant table is walked through a materialized local (the compiler
// rejects variable indexing straight into constant data).
protocol_version_supported :: proc(v: string) -> bool {
	supported := PROTOCOL_SUPPORTED
	for i in 0..<len(supported) {
		if v == supported[i] {
			return true
		}
	}
	return false
}
