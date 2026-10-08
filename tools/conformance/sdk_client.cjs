// Reference MCP client over stdio, built on the official TypeScript SDK.
// Run under NODE_PATH pointing at a node_modules carrying
// @modelcontextprotocol/sdk — `just conformance` resolves one.
// Usage: sdk_client.cjs SERVER_CMD [SERVER_ARGS...]
// The expected peer is tools/conformance (mode: serve).
const { Client } = require('@modelcontextprotocol/sdk/client/index.js');
const { StdioClientTransport } = require('@modelcontextprotocol/sdk/client/stdio.js');

const command = process.argv[2];
const args = process.argv.slice(3);
if (!command) {
  console.error('usage: sdk_client.cjs SERVER_CMD [SERVER_ARGS...]');
  process.exit(2);
}

let passed = 0;
let failed = 0;
const check = (ok, what, detail = '') => {
  const n = passed + failed + 1;
  if (ok) {
    passed += 1;
    console.log(`ok ${n}  ${what}`);
  } else {
    failed += 1;
    console.log(`FAIL ${n}  ${what}${detail ? `: ${detail}` : ''}`);
  }
};

// Hard exit bound: the driver must never outlive a wedged peer's close.
setTimeout(() => {
  console.error('sdk_client: close did not finish; hard exit');
  process.exit(1);
}, 15_000).unref();

(async () => {
  const transport = new StdioClientTransport({ command, args, stderr: 'inherit' });
  const client = new Client({ name: 'sdk-reference-client', version: '1.26.0' });
  try {
    await client.connect(transport);

    // The SDK validates the negotiated protocolVersion inside connect()
    // (it throws on a revision outside its supported set), so reaching
    // this line already proves the handshake; assert the serverInfo
    // identity the SDK hands back.
    const version = client.getServerVersion();
    check(version?.name === 'odin-mcp-conformance' && version?.version === '0.1.0',
      'initialize identifies the peer', JSON.stringify(version));

    const caps = client.getServerCapabilities();
    check(caps?.tools?.listChanged === true, 'the tools capability advertises listChanged');

    const listed = await client.listTools();
    check(Array.isArray(listed.tools) && listed.tools.some((t) => t.name === 'echo'),
      'tools/list carries echo', `${listed.tools?.length} tools`);

    const echoed = await client.callTool({ name: 'echo', arguments: { text: 'conformance' } });
    check(echoed.content?.[0]?.text === 'echo:conformance' && !echoed.isError,
      'tools/call echo round-trips', JSON.stringify(echoed));

    // Input validation lands as an isError tool result, never -32602.
    const invalid = await client.callTool({ name: 'echo', arguments: {} });
    check(invalid.isError === true, 'missing-argument echo answers isError', JSON.stringify(invalid));

    // An unknown tool is a protocol-level -32602; the SDK surfaces it as
    // a thrown McpError.
    let unknown = null;
    try { await client.callTool({ name: 'nope', arguments: {} }); } catch (e) { unknown = e; }
    check(unknown?.code === -32602, 'the unknown tool answers -32602', String(unknown));

    // The deferred slow call: the client-side timeout cancels the request
    // (notifications/cancelled crosses; the host answers -32800, which
    // the SDK ignores for the abandoned id).
    let timed = null;
    try {
      await client.callTool({ name: 'slow', arguments: {} }, undefined, { timeout: 400 });
    } catch (e) { timed = e; }
    check(!!timed, 'the slow call is cut short by the client timeout', String(timed));

    await client.ping();
    const after = await client.callTool({ name: 'echo', arguments: { text: 'after-cancel' } });
    check(after.content?.[0]?.text === 'echo:after-cancel',
      'the session stays healthy after a cancelled call');
  } catch (e) {
    check(false, 'unexpected failure', String(e));
  } finally {
    await client.close();
  }

  console.log(`${failed === 0 ? 'PASS' : 'FAIL'}: ${passed} ok, ${failed} failed`);
  process.exit(failed === 0 ? 0 : 1);
})();
