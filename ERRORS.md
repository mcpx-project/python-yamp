# yamp Error Index

Every error yamp emits carries a stable error id in `data.errorId`, and that
id is the key to this index. The leading digit is the error class: `E4xxx` is a
client-caused error and `E5xxx` is a server-side one. The remaining digits echo
the nearest HTTP status where one exists. Backend errors pass through unnamed, so
they are not listed here.

This file is generated from the single-source registry in the `errors` module
(both arms). Do not edit it by hand; regenerate it with
`python/tools/gen_error_index.py`.

| Error ID | JSON-RPC code | Reason |
| --- | --- | --- |
| [E4000](#e4000) | `-32600` | Invalid Request |
| [E4002](#e4002) | `-32602` | Invalid Params |
| [E4004](#e4004) | `-32601` | Method Not Found |
| [E4010](#e4010) | `-32002` | Unauthorized |
| [E4030](#e4030) | `-32001` | Policy Denied |
| [E4400](#e4400) | `-32000` | No Session |
| [E4260](#e4260) | `-32004` | Unsupported Protocol Version |
| [E4006](#e4006) | `-32005` | Header Mismatch |
| [E5000](#e5000) | `-32603` | Internal Error |
| [E5030](#e5030) | `-32003` | Server Not Available |

### E4000

**Invalid Request** (JSON-RPC code `-32600`)

Cause. The message is not a well-formed JSON-RPC request object.

Fix. Send a JSON-RPC 2.0 request carrying jsonrpc, method, and id.

### E4002

**Invalid Params** (JSON-RPC code `-32602`)

Cause. The request's params do not match the method's expected shape.

Fix. Check the tool's inputSchema and correct the arguments.

### E4004

**Method Not Found** (JSON-RPC code `-32601`)

Cause. The requested method or namespaced tool is exposed by no backend or handler.

Fix. List the available tools and call one of the exposed names.

### E4010

**Unauthorized** (JSON-RPC code `-32002`)

Cause. The client's token failed issuer or audience validation, or none was presented.

Fix. Present a token whose iss and aud match the proxy's configured values.

### E4030

**Policy Denied** (JSON-RPC code `-32001`)

Cause. A policy rule or extension filter denied the request.

Fix. Review the active policy and filter chain, then adjust the rule or the request.

### E4400

**No Session** (JSON-RPC code `-32000`)

Cause. The request referenced a session that is missing or has expired.

Fix. Reinitialize to obtain a session before sending session-scoped requests.

### E4260

**Unsupported Protocol Version** (JSON-RPC code `-32004`)

Cause. The declared protocol version is not in the proxy's supported set.

Fix. Negotiate one of the supported versions named in the error data.

### E4006

**Header Mismatch** (JSON-RPC code `-32005`)

Cause. A transport routing header disagrees with the request body.

Fix. Make Mcp-Method and Mcp-Name agree with the body, or omit them.

### E5000

**Internal Error** (JSON-RPC code `-32603`)

Cause. The proxy or a local handler failed while processing the request.

Fix. Retry; if it persists, check the server logs and report the error id.

### E5030

**Server Not Available** (JSON-RPC code `-32003`)

Cause. The target backend is unavailable or its circuit breaker is open.

Fix. Wait for the backend to recover; the proxy resumes once it is healthy.
