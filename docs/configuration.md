# Configuration

signal-hub reads one YAML file (`SIGNAL_HUB_CONFIG`, default `/etc/signal-hub/config.yaml`). Unknown keys fail the start. A full example is in [`examples/config.yaml`](../examples/config.yaml).

## Process environment

| Variable | Meaning |
| --- | --- |
| `SIGNAL_HUB_CONFIG` | config file path, default `/etc/signal-hub/config.yaml` |
| `SIGNAL_HUB_ACCOUNT` | the bot's number (E.164); overrides `upstream.accountFile` and `upstream.account` |
| `SIGNAL_HUB_LOG_LEVEL` | `debug`, `info` (default), `warn`, `error` |
| `SIGNAL_HUB_LOG_FORMAT` | `json` (default) or `text`; logs go to stdout |

## Keys

| Key | Default | Notes |
| --- | --- | --- |
| `version` | | required, must be `1` |
| `upstream.url` | | required, http or https URL of signal-cli-rest-api |
| `upstream.account` | | the bot's number; not together with `accountFile` |
| `upstream.accountFile` | | file holding the bot's number (trimmed) |
| `listen` | `:8080` | generic API, bearer token required |
| `adminListen` | `:9090` | `/healthz`, `/readyz`, `/metrics`, no auth |
| `database.url` | `sqlite:///var/lib/signal-hub/hub.db` | only the `sqlite` scheme with an absolute path is supported; its directory is the volume |
| `retention` | `14d` | how long messages are kept |
| `tokenDir` | `/etc/signal-hub/tokens` | one file per consumer |
| `limits.maxRequestBytes` | `26214400` | body limit for `POST /v1/send` and compat RPC requests, and size limit for compat local attachments |
| `consumers` | | at least one |

Durations use Go syntax (`90s`, `10m`, `24h`) or whole days (`7d`).

### Consumers

| Key | Default | Notes |
| --- | --- | --- |
| `name` | | `^[a-z0-9][a-z0-9-]{0,31}$`, unique |
| `groups` | | internal id or `group.<base64>`; stored as the internal id |
| `senders` | | E.164 numbers or UUIDs (lower-cased); apply to direct messages only |
| `maxReplayAge` | `min(24h, retention)` | greater than 0 and at most `retention` |
| `compat.listen` | off | enables the signal-cli daemon contract on that address, no auth |
| `compat.attachmentDir` | empty | absolute directory that compat `send` may read local attachment paths from; empty refuses every local path |

Each consumer lists at least one group or sender. A group or sender belongs to at most one consumer, compared after normalization. `listen`, `adminListen` and every `compat.listen` must be distinct. Group messages route by group only.

All problems are reported together, so one failed start shows every error.

## The bot's number

The number is resolved in this order: `SIGNAL_HUB_ACCOUNT`, then the trimmed contents of `upstream.accountFile`, then `upstream.account`. Set `account` or `accountFile`, not both. When the environment variable is set, `accountFile` is not read.

To keep the number out of the config, which can then be a reviewable ConfigMap:

- mount a Secret key as a file and point `upstream.accountFile` at it, or
- set `SIGNAL_HUB_ACCOUNT` from a Secret.

The number is never logged, and validation errors name the failing source, not the value.

## Tokens

Each consumer's token is the file `<tokenDir>/<consumer name>`, trimmed, at least 32 characters, unique across consumers. Mount the token Secret at `tokenDir` with one key per consumer name:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: signal-hub-tokens
stringData:
  dsh: <at least 32 random characters>
  ops: <at least 32 random characters>
```

A token is required unless the consumer has `compat`. Without a token a compat consumer has no generic-API access.
