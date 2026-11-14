# Operations

## Ports and who may reach them

| Port | Default | May be reached by |
| --- | --- | --- |
| API (`listen`) | 8080 | the consumers' pods only; every call needs the consumer's bearer token |
| Admin (`adminListen`) | 9090 | monitoring only (Prometheus, Gatus); no authentication, no consumer data beyond counters and positions |
| Compat (`consumers[].compat.listen`) | none | that consumer's pods only; no authentication, the port decides the consumer |

Outbound, the hub connects to `upstream.url` (signal-cli-rest-api, HTTP and WebSocket) and nothing else. Restrict the ports with a NetworkPolicy or equivalent; the compat port in particular is open to anything that can reach it.

## Volume and backup

The store is one SQLite file, `sqlite:///var/lib/signal-hub/hub.db` by default, written through a pure Go driver (no CGO). Postgres is not supported in v1.

- Mount a persistent volume at the database's directory. The image runs as 65532 and creates the directory with that owner, so a fresh volume works.
- Without a volume the log, dedupe keys and consumer positions are lost on restart.
- Run a single replica; the database is not shared between processes.
- Back up by copying the database files with the pod stopped, or online with `sqlite3 /var/lib/signal-hub/hub.db ".backup '/backup/hub.db'"` (the image has no shell; run it from a debug container with the volume mounted).
- Messages older than `retention` are pruned hourly.

## Health

`GET /healthz` on the admin port answers 200 when the upstream is connected and the store is writable, otherwise 503, with the same body:

```json
{
  "status": "ok",
  "upstream": {"connected": true, "connectedSince": "2026-11-14T08:00:00Z", "reconnects": 1},
  "store": {"writable": true, "error": ""},
  "lastMessageAt": "2026-11-14T09:12:41Z",
  "lastMessageAgeSeconds": 33,
  "unclaimed": 0,
  "missed": {"dsh": 0},
  "consumers": {"dsh": {"events": {"subscribers": 1, "position": 812}}}
}
```

`status` is `ok` or `error`. `lastMessageAt` and `lastMessageAgeSeconds` are `null` until a message was stored.

`GET /readyz` answers 200 `ok` once the store is open and 503 `not ready` before that and during shutdown.

Gatus:

```yaml
endpoints:
  - name: signal-hub
    url: http://signal-hub.tools.svc.cluster.local:9090/healthz
    conditions:
      - "[STATUS] == 200"
```

## Metrics

`GET /metrics` on the admin port, Prometheus text format.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `signal_hub_envelopes_total` | counter | `kind` | envelopes stored |
| `signal_hub_duplicates_total` | counter | | envelopes dropped as duplicates |
| `signal_hub_frames_rejected_total` | counter | | upstream frames rejected |
| `signal_hub_unclaimed_total` | counter | | messages no consumer claimed |
| `signal_hub_missed_total` | counter | `consumer` | messages older than `maxReplayAge` that a consumer skipped |
| `signal_hub_delivered_total` | counter | `consumer`, `channel` | messages delivered |
| `signal_hub_sends_total` | counter | `consumer`, `result` | outbound sends by result, including `denied` |
| `signal_hub_upstream_connected` | gauge | | 1 while the upstream WebSocket is connected |
| `signal_hub_upstream_reconnects_total` | counter | | upstream reconnects |
| `signal_hub_last_message_timestamp_seconds` | gauge | | Unix time of the last stored message |
| `signal_hub_store_writable` | gauge | | 1 while the store accepts writes |
| `signal_hub_pruned_total` | counter | | messages pruned by retention |
| `signal_hub_consumer_position` | gauge | `consumer`, `channel` | last delivered seq |

## Log messages

Logs are JSON on stdout by default (`SIGNAL_HUB_LOG_FORMAT`, `SIGNAL_HUB_LOG_LEVEL`). The bot's number is redacted from every logged URL and error.

### Upstream

| Message | Level | Meaning and action |
| --- | --- | --- |
| `upstream connected` | info | WebSocket established (`url`, `attempt`). Nothing to do. |
| `upstream dial failed` | warn | The connection attempt failed (`error`, `attempt`). The hub retries with backoff. If it persists, check `upstream.url` and signal-cli-rest-api. |
| `upstream disconnected` | warn | An established connection ended (`error`, `uptime`). The hub reconnects with backoff; a short `uptime` repeating means a flapping upstream. |
| `frame rejected` | warn | A frame was not a valid envelope and was dropped (`error`, `bytes`); `signal_hub_frames_rejected_total` counts it. A steady stream points to a signal-cli-rest-api version the hub was not built for. |
| `store append failed` | error | Writing to the database failed (`attempt`, `error`). The hub retries with backoff and holds the frame; `/healthz` turns 503. Check free space, volume permissions and the mount. |

### Routing

| Message | Level | Meaning and action |
| --- | --- | --- |
| `unclaimed message` | info | A message matched no consumer (`kind` and `group` or `sender`). Logged once per group or sender per day and kept in the log. Add the group or sender to a consumer if it should be delivered. |
| `unclaimed messages suppressed` | info | The per-day list of logged keys reached its limit (`limit`); further unclaimed messages are only counted. |

### Delivery

| Message | Level | Meaning and action |
| --- | --- | --- |
| `messages missed` | warn | A consumer reconnected after longer than its `maxReplayAge` and skipped older messages (`consumer`, `channel`, `count`, `maxReplayAge`). Raise `maxReplayAge` (up to `retention`) if they must not be lost. |
| `cursor ahead of log` | warn | A consumer's cursor or stored position is beyond the log head, which means the database was replaced or wiped (`consumer`, `channel`, `after`, `head`). The consumer restarts from the oldest retained message. |
| `events write deadline` | warn | Setting the write deadline on an event stream failed (`consumer`). The stream continues without a deadline. |
| `events read failed` | error | Reading the log for a stream failed (`consumer`, `error`). The stream ends; the client reconnects. |
| `events position failed` | error | Storing a stream position failed (`consumer`, `error`). Events may repeat after a reconnect. Check the store. |
| `message build failed` | error | A stored record could not be turned into the neutral format (`consumer`, `seq`). The record is skipped. |
| `message encode failed` | error | A message could not be encoded for an event stream (`consumer`, `seq`). |
| `request failed` | error | A pull request failed (`consumer`, `op`, `error`). Check the store. |
| `attachment fetch failed` | error | signal-cli-rest-api failed to serve an attachment (`consumer`, `seq`, `error`). The client got 502. |
| `attachment stream aborted` | warn | The attachment download broke off mid-stream (`consumer`, `seq`). |
| `attachment lookup failed` | error | Compat `getAttachment` could not look the id up in the store (`consumer`, `error`). |

### Sending

| Message | Level | Meaning and action |
| --- | --- | --- |
| `send denied` | warn | A consumer tried to send to a recipient outside its groups and senders (`consumer`, `recipient`); nothing was sent. Fix the client or the consumer's lists. |
| `send failed` | error | signal-cli-rest-api rejected or failed a send (`consumer`, `error`). 429 means rate limiting; retry later. |

### Retention

| Message | Level | Meaning and action |
| --- | --- | --- |
| `pruned` | info | The hourly retention pass finished (`count` deleted, `before` cutoff). Nothing to do. |
| `prune failed` | error | The retention pass failed (`error`). Retried an hour later; the log grows meanwhile. Check the store. |
| `store not writable` | error | The 30-second write probe failed (`error`); `/healthz` turns 503 until a probe or an append succeeds. Check free space and volume permissions. |

### Lifecycle

| Message | Level | Meaning and action |
| --- | --- | --- |
| `starting` | info | Process start (`version`, `config`). |
| `listening` | info | A listener is bound (`name`, `addr`). |
| `shutting down` | info | A termination signal arrived; `/readyz` turns 503 and the servers drain. |
| `listener failed` | error | A listener stopped unexpectedly (`error`); the hub exits non-zero. |
| `server shutdown` | warn | A server did not drain cleanly (`name`, `error`). |
| `ingester did not stop within the exit budget` | warn | The upstream reader was still running 15 seconds after the signal. |
| `close store` | error | Closing the database failed (`error`). |
| `stopped` | info | Clean exit. |

A configuration or startup error is printed to stderr as `signal-hub: <error>` and the process exits non-zero.

## Compat attachments

Compat `send` reads local attachment paths only from the consumer's `compat.attachmentDir`. The directory must exist inside the hub's container (mount it read-only), and the client must send the same absolute path. With no `attachmentDir` every local path is refused, and a refused path answers like a missing file.
