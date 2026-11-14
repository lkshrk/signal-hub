# Generic API

The API listens on `listen` (default `:8080`). Every endpoint needs `Authorization: Bearer <token>`, where the token is the consumer's token from `tokenDir`.

Errors are `{"error":"<text>"}` with the status listed. A missing, malformed or unknown token is 401 `{"error":"unauthorized"}` with `WWW-Authenticate: Bearer`.

## Pull

`GET /v1/messages?after=<seq>&wait=<s>&limit=<n>`

The client owns the cursor.

| Parameter | Meaning |
|-----------|---------|
| `after` | Absent: start at the log head (live from now). `0`: from the oldest retained message. Otherwise the `next` of the previous call. |
| `wait` | Seconds to hold the request open until a message arrives, 0-60 (clamped), default 0. |
| `limit` | Messages per response, 1-500 (clamped), default 100. |

Response 200:

```json
{"messages": [], "next": 42, "missed": 0, "reset": false}
```

- `messages` holds only the consumer's routed messages of the delivered kinds (`message`, `edit`, `reaction`, `pollVote`), in `seq` order, in the [neutral format](message-format.md).
- `next` is the cursor for the following call. It advances past rows that were skipped.
- Messages received longer ago than the consumer's `maxReplayAge` are not delivered. They are counted in `missed`, logged and counted in `signal_hub_missed_total{consumer}`.
- `after` greater than the log head (the log was wiped) sets `reset: true` and reads from the oldest retained message.
- With `wait`, an empty response after the wait expires returns `next` unchanged.
- 400 for an `after` that is not a non-negative integer, or a `wait` or `limit` that is not an integer.

## Events

`GET /v1/events?after=<seq>` (Server-Sent Events)

The server keeps the consumer's position (channel `events`), so a reconnect without a cursor continues where the last delivered event left off.

Start position, first match wins:

1. `Last-Event-ID` header (sent by EventSource on reconnect).
2. `after` query parameter.
3. The consumer's stored position.
4. The log head (live from now).

Stream:

```
: connected

id: 43
event: message
data: {"seq":43,...}

: keepalive
```

- `data` is one message in the [neutral format](message-format.md); `id` is its `seq`.
- A gap over `maxReplayAge` is announced first as `event: missed` with `data: {"count":<n>}`. It is logged and counted like in pull, with channel `events`.
- `: keepalive` is sent every 25 seconds.
- The stored position advances only after an event was written and flushed. It never moves backwards.
- Delivery is at least once: a missed notice or an event can repeat after a failed write.
- Resume with `Last-Event-ID`: the stored position can be one event ahead of what a dropped client actually received.
- 400 for a `Last-Event-ID` or `after` that is not a non-negative integer.

## Attachments

`GET /v1/attachments/{id}`

Streams the attachment bytes from signal-cli-rest-api with the stored `Content-Type` and the upstream `Content-Length` when known. The `id` is `attachments[].id` of a pulled message.

- 404 `{"error":"not found"}` unless the id belongs to a message routed to this consumer. An id of another consumer is indistinguishable from an unknown one.
- 502 `{"error":"upstream error"}` when signal-cli-rest-api fails.
