# signal-hub

Durable Signal message hub in front of signal-cli-rest-api: it persists incoming messages, normalizes group ids and serves them to consumers over an HTTP API.

## Why

signal-cli-rest-api allows one WebSocket reader per account and keeps no history. signal-hub holds that single connection, appends every envelope to a durable log (SQLite, pure Go) and delivers from the log to each consumer by pull, Server-Sent Events or a signal-cli daemon compatibility listener. Each consumer sees only its own groups and senders, keeps its own position and catches up after a restart. Outbound messages go through the same hub, limited to the consumer's own groups and senders.

## Quick start

```sh
mkdir -p hub/tokens
cp examples/config.yaml hub/config.yaml
head -c 32 /dev/urandom | base64 > hub/tokens/dsh

docker run --rm \
  -e SIGNAL_HUB_ACCOUNT=+4915112345678 \
  -v "$PWD/hub/config.yaml:/etc/signal-hub/config.yaml:ro" \
  -v "$PWD/hub/tokens:/etc/signal-hub/tokens:ro" \
  -v signal-hub-data:/var/lib/signal-hub \
  -p 8080:8080 -p 9090:9090 \
  ghcr.io/lkshrk/signal-hub:latest
```

- The image runs as user 65532. The `signal-hub-data` volume holds the SQLite database and is created writable for that user.
- Edit `upstream.url` and the consumers in `config.yaml` first; one token file per consumer, named after it.
- `curl localhost:9090/healthz` reports the state, and `curl -H "Authorization: Bearer $(cat hub/tokens/dsh)" "localhost:8080/v1/messages?wait=30"` pulls messages.

## Documentation

- [Configuration](docs/configuration.md)
- [Generic API](docs/api.md)
- [Message format](docs/message-format.md)
- [Compatibility mode](docs/compatibility.md)
- [Operations](docs/operations.md)

## Development

```sh
mise install
lefthook install
go test -race ./...
```

## Release

```sh
git tag vX.Y.Z && git push --tags
```

The tag must be on `main`. The Release workflow tests, builds `linux/amd64` and `linux/arm64`, pushes `ghcr.io/lkshrk/signal-hub` as `X.Y.Z`, `X.Y` and `sha-<short>` (plus `latest` for a non-prerelease tag) and creates a GitHub release that names the image digest.

## License

[MIT](LICENSE)
