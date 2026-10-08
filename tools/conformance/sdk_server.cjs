// Reference MCP server over stdio, built on the official TypeScript SDK.
// Run under NODE_PATH pointing at a node_modules carrying
// @modelcontextprotocol/sdk — `just conformance` resolves one. The peer
// on the other side is tools/conformance (mode: client).
const { Server } = require('@modelcontextprotocol/sdk/server/index.js');
const { StdioServerTransport } = require('@modelcontextprotocol/sdk/server/stdio.js');
const {
  ListToolsRequestSchema,
  CallToolRequestSchema,
  McpError,
} = require('@modelcontextprotocol/sdk/types.js');

const server = new Server(
  { name: 'sdk-reference', version: '1.26.0' },
  { capabilities: { tools: {} } },
);

server.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: [
    {
      name: 'echo',
      description: 'echoes its text argument',
      inputSchema: {
        type: 'object',
        properties: { text: { type: 'string' } },
        required: ['text'],
      },
    },
    {
      name: 'slow',
      description: 'sleeps for a minute; the client cancels it',
      inputSchema: { type: 'object', properties: {} },
    },
  ],
}));

// Aborted through notifications/cancelled: the SDK aborts the handler's
// signal and sends no response for the cancelled request.
const sleep = (ms, signal) =>
  new Promise((resolve, reject) => {
    const timer = setTimeout(() => resolve('slept'), ms);
    if (signal) {
      signal.addEventListener('abort', () => {
        clearTimeout(timer);
        reject(signal.reason);
      }, { once: true });
    }
  });

server.setRequestHandler(CallToolRequestSchema, async (request, extra) => {
  const { name } = request.params;
  const args = request.params.arguments;
  switch (name) {
    case 'echo': {
      const text = args?.text;
      if (typeof text !== 'string' || text === '') {
        // Input validation fails as an isError tool result, never -32602.
        return {
          content: [{ type: 'text', text: 'echo requires a non-empty text argument' }],
          isError: true,
        };
      }
      return { content: [{ type: 'text', text: `echo:${text}` }] };
    }
    case 'slow': {
      await sleep(60_000, extra.signal);
      return { content: [{ type: 'text', text: 'slept' }] };
    }
    default:
      // An unknown tool is a protocol-level error, never a tool result.
      throw new McpError(-32602, `Unknown tool: ${name}`);
  }
});

// stdin EOF is the client going away: the driver's teardown ladder
// depends on the process actually exiting.
server.onclose = () => process.exit(0);

(async () => {
  await server.connect(new StdioServerTransport());
})().catch((err) => {
  console.error('sdk_server:', err);
  process.exit(1);
});
