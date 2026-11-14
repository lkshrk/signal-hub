# Compatibility mode

A consumer with `compat.listen` gets a second listener that serves the subset of the signal-cli HTTP daemon API that the Hermes Signal adapter uses. It is the contract of the `signal-shim` sidecar (agent-stack `cmd/signal-shim` at commit `d9f6594`), so a client switches by changing one URL. The listener has no authentication: the port decides the consumer.

The contract is verified against 23 recordings of the shim's own responses in `internal/compat/testdata/golden`.

## Kept contract

- `GET /api/v1/check`: 200 `application/json` `{"versions":{"signal-cli":"via signal-cli-rest-api"},"upstream":<upstream /v1/about body>}`. An upstream failure is 502 with the error text.
- `GET /api/v1/events?account=<account>`: a missing `account` is 400 `account query parameter required`. The stream has `Content-Type: text/event-stream` and `Cache-Control: no-cache`, starts with `: connected`, and sends each envelope as `data: <compact upstream frame>` with no `id:` or `event:` lines. A poll vote without text gets `dataMessage.message` set to the vote text, for example `🗳️ Poll "Start now?": ✅ Start (poll 1763114850000)`. A comment `: keepalive` follows every 25 s without traffic.
- `POST /api/v1/rpc`, JSON-RPC 2.0, response `{"id","jsonrpc":"2.0","result"|"error"}`:

| Method | Upstream call | Result |
|--------|---------------|--------|
| `send` | `POST /v2/send` | `{"results":[{"type":"SUCCESS"}],"timestamp":<int>}` |
| `sendTyping` | `PUT` (or `DELETE` with `stop`) `/v1/typing-indicator/<account>` per recipient | `{}` |
| `sendReaction` | `POST` (or `DELETE` with `remove`) `/v1/reactions/<account>` per recipient | `{}` |
| `sendPollCreate` | `POST /v1/polls/<account>` | `{"timestamp":<int>}` |
| `sendPollTerminate` | `DELETE /v1/polls/<account>` | `{}` |
| `getAttachment` | `GET /v1/attachments/<id>` | `{"data":"<base64>"}` |
| `getContact` | none | `{}` |
| `listContacts` | none | `[]` |

- Error codes: -32700 parse error, -32602 invalid params, -32601 unsupported method, -32603 internal, 429 rate limited (upstream status 429 or a body containing `RateLimit`). Upstream errors read `<METHOD> <path> returned HTTP <code>: <first 300 bytes of the body>`.
- `groupId` (the signal-cli internal id) maps to `group.` + base64 of the id. `textStyle`/`textStyles` ranges render as styled text; `quoteTimestamp` and `quoteAuthor` pass through.
- Local `attachments` paths are inlined as data URIs. A path the hub cannot read fails with `read attachment <name>: <error>`.
- The last 256 polls created through compat mode are remembered for vote text. Polls are kept in memory, so a restart forgets them and votes fall back to `🗳️ Poll vote: option <n> (poll <ts>)`.

## Deliberate differences

| Shim | Hub |
|------|-----|
| `method not supported by signal-shim: <m>` | `method not supported by signal-hub: <m>` |
| Any recipient or group is forwarded | A target outside the consumer's groups and senders is refused with -32603 `recipient not allowed for consumer <name>`; nothing is sent |
| `account` is forwarded as given | An `account` other than the hub's is refused with -32603 `account <x> is not served by this hub`; a missing one uses the hub's account without calling `/v1/accounts` |
| `getAttachment` fetches any id | An id that is unknown or belongs to another consumer answers like an id upstream does not know, without calling upstream: -32603 `GET /v1/attachments/<id> returned HTTP 404: {"error":"No attachment with that name found"}` |
| Events relay the upstream WebSocket and end when it drops | Events come from the log: the stream stays open while upstream is down, carries only the consumer's routed envelopes (all kinds, including receipts and typing), and resumes from the server-side position `compat` |
| Missed events while disconnected are lost | Envelopes newer than the consumer's `maxReplayAge` are replayed on reconnect; older ones are skipped, logged as `messages missed` with channel `compat` and counted in `signal_hub_missed_total` |
| `/api/v1/events` with any account | Another account is 404 `unknown account` |
| Upstream error texts show the bot number | The bot number is shown as `<account>` in upstream error texts |
| A transport error reads `<METHOD> <path>: Get "<url>": <cause>` | It reads `<METHOD> <path>: <cause>` |

None of the 23 recordings contain the bot number in an upstream error text, so no recorded expectation needed the redacted form.

## Migration

1. Give the consumer a `compat.listen` address, for example `:8090`.
2. Point the client's `SIGNAL_HTTP_URL` at `http://<hub>:<compat port>`. Keep its account setting; it must equal the hub's account or be empty.
3. Remove the shim sidecar.

The hub reads local attachment paths for outgoing attachments, so a path must exist inside the hub's container. Today the Hermes sidecar shim cannot read Hermes' files either and fails with the same error.

On the first connect the stream starts at the log head, as the shim did. After that a reconnect resumes where the last stream stopped.
