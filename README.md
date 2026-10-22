# Daployi Go plugin SDK

A native Go subprocess SDK for protocol version 1. The agent owns cloud communication, authentication, and the durable measurement queue. The plugin receives only its assigned configuration through private stdin/stdout pipes; it does not open an RPC port.

## Local development

This project currently retains its original local module name, `daployi-plugin-sdk-go`. In a separate plugin project, add:

```sh
go mod edit -require=daployi-plugin-sdk-go@v0.0.0
go mod edit -replace=daployi-plugin-sdk-go=/Users/dustinauby/Documents/Projects/Go/daployi-plugin-sdk-go
```

Import it as `plugin "daployi-plugin-sdk-go"`. Replace the local module path with the published repository/module path before distributing the SDK externally. Go 1.26 is required by the current module.

See `examples/temperature/main.go` for a compilable collector. Build with:

```sh
go build -o temperature-plugin ./examples/temperature
```

The executable waits for an agent handshake. Running it directly from a terminal does not initialize it. The licensing build pipeline must sign each platform binary and the web system must explicitly assign it to a device before the agent executes it. Copying a binary into a directory never enables it.

## Lifecycle and methods

Call `plugin.Run(ctx, handler)`, decode the JSON configuration supplied to the handler, initialize the integration, then call `client.Ready(ctx)` once. Ready changes the agent's plugin state to `running`. Return an error if initialization fails. Observe the handler context cancellation, close devices/connections, and return promptly on shutdown. The host kills processes that exceed its shutdown timeout.

Standard output and standard input are exclusively reserved for the RPC protocol. Diagnostic output belongs on stderr. The initial agent implementation discards plugin stderr rather than leaking connection secrets into shared agent logs.

| Method | Result and guarantees |
| --- | --- |
| `PublishData(ctx, stream, records...)` | Returns accepted IDs only after an atomic SQLite commit on the agent. The agent forwards those records and removes them only after explicit server acknowledgment. |
| `ReviewData(ctx, stream, record)` | Evaluates configured alert rules for this plugin and stream. Returns the number evaluated. Does not queue or ingest the measurement. Alert notifications use the agent's existing alert reporting system. |
| `PushLiveData(ctx, stream, records...)` | Attempts a one-shot event on the current approved WebSocket. `Accepted` means admission to a bounded, expiring memory buffer, not delivery to a server/browser. No persistence, retry, or replay across connections. |

Publishing does not automatically review or emit a live event. Call each method deliberately when all three behaviors are needed.

`plugin.NewRecord(payload)` produces a random record ID, UTC timestamp, and JSON payload. For source timestamps, replace `record.Timestamp` before submitting. Stable IDs are essential: retry uncertain publish outcomes with the same ID, timestamp, and payload. Delivery is **at least once**; the server must deduplicate by authenticated device, plugin, stream, and record ID, including retries after local acknowledgment deletion. Do not turn a timeout into a new record ID.

Methods are safe for concurrent calls, but keep concurrency at eight or fewer requests per subprocess. Always supply a timeout/cancellable context. Cancellation while blocked writing closes the session to release the pipe. A cancellation after sending cannot undo an already accepted request. Reusing an ID with changed content while queued is rejected.

Limits:

- Streams and record IDs: 1–128 ASCII characters, starting with a letter or number; subsequent characters may also be `.`, `_`, `-`.
- Publish/live batches: 1–100 records, at most 512 KiB serialized data; live requests have a tighter 64 KiB limit.
- Configuration: JSON object, at most 64 KiB. IPC frames: less than 1 MiB.
- Review: exactly one record, JSON object payload, no older than two minutes or more than five seconds ahead. Duplicate/out-of-order observation timestamps do not change alert state.
- Publish: source timestamps may be historical, but cannot be zero or over five minutes ahead.
- Queue defaults: 30 days from agent acceptance, 100,000 records, 256 MiB of serialized record bodies. Expired records are discarded; a full unexpired queue rejects new writes with `*plugin.Error{Code: "queue_full"}`. Handle backpressure explicitly. The example logs collection failures and does not provide its own retry buffer.

Retention is controlled on the agent with `DAPLOYI_PLUGIN_RETENTION_DAYS`; plugins cannot override it. Disk faults/limits can reject a publish. A successful publish does not guarantee eventual delivery beyond the retention window.

## Protocol

Messages are newline-delimited UTF-8 JSON. Each contains `version: 1`. The host sends `initialize` with `{pluginId, release, config}`; the client sends `ready` and receives a correlated response. Requests use a string `id`, `method`, and `params`; responses carry the same ID and either `result` or `{error:{code,message}}`.

Data method parameters:

```json
{"stream":"temperature","records":[{"id":"sample-123","timestamp":"2026-10-20T08:00:00Z","payload":{"temperature":27}}]}
```

Method names are `publishData`, `reviewData`, and `pushLiveData`. Results are respectively `{acceptedIds:[...]}`, `{evaluated:1}`, and `{accepted:true}`. The SDK handles host `ping` → `pong` and `shutdown` automatically. Plugin identity comes from the host's assigned process, never from a request field supplied by the plugin.

## Tests

```sh
go test -race ./...
go build -o /tmp/daployi-plugin-probe ./testdata/probe
```

From the agent project, run `DAPLOYI_TEST_PLUGIN=/tmp/daployi-plugin-probe go test -race ./internal/plugins` to exercise the SDK against the real subprocess host, signature checks, SQLite queue, and disable lifecycle.
