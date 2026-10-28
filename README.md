# Daployi Go plugin SDK

A native Go subprocess SDK for protocol version 1. The agent owns cloud communication, authentication, and the durable measurement queue. The plugin receives only its assigned configuration through private stdin/stdout pipes; it does not open an RPC port.

## Installation and local development

The module path is `github.com/Daployi/daployi-plugin-sdk-go` (Go 1.26).
Import it as `plugin "github.com/Daployi/daployi-plugin-sdk-go"`.

Actions require an SDK release containing `Client.Action`; v0.0.2 predates this
API. Until a new version is tagged and published, use the local SDK checkout:

```sh
go mod edit -require=github.com/Daployi/daployi-plugin-sdk-go@v0.0.2
go mod edit -replace=github.com/Daployi/daployi-plugin-sdk-go=/Users/dustinauby/Documents/Projects/Go/daployi-plugin-sdk-go
go mod tidy
```

After publishing, remove the local replacement and use `go get` with that new
version. Do not move or overwrite the existing v0.0.2 tag.

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
| `PublishData(ctx, stream, records...)` | Returns durable `AcceptedIDs` and optional `SuppressedIDs` for unchanged readings when the device assignment enables change reporting. Only durable records are forwarded and removed after explicit server acknowledgment. |
| `ReviewData(ctx, stream, record)` | Evaluates configured alert rules for this plugin and stream. Returns the number evaluated. Does not queue or ingest the measurement. Alert notifications use the agent's existing alert reporting system. |
| `PushLiveData(ctx, stream, records...)` | Attempts a one-shot event on the current approved WebSocket. `Accepted` means admission to a bounded, expiring memory buffer, not delivery to a server/browser. No persistence, retry, or replay across connections. |

Publishing does not automatically review or emit a live event. Call each method deliberately when all three behaviors are needed.

`plugin.NewRecord(payload)` produces a random record ID, UTC timestamp, and JSON payload. For source timestamps, replace `record.Timestamp` before submitting. Stable IDs are essential: retry uncertain publish outcomes with the same ID, timestamp, and payload. Delivery is **at least once**; the server must deduplicate by authenticated device, plugin, stream, and record ID, including retries after local acknowledgment deletion. Do not turn a timeout into a new record ID.

Methods are safe for concurrent calls, but keep concurrency at eight or fewer requests per subprocess. Always supply a timeout/cancellable context. Cancellation while blocked writing closes the session to release the pipe. A cancellation after sending cannot undo an already accepted request. Reusing an ID with changed content while queued is rejected.

Limits:

- Streams and record IDs: 1–128 ASCII characters, starting with a letter or number; subsequent characters may also be `.`, `_`, `-`.
- Publish/live batches: 1–100 records, at most 512 KiB serialized data; live requests have a tighter 64 KiB limit.
- Configuration: JSON object, at most 64 KiB. IPC frames: less than 16 MiB.
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

Method names are `publishData`, `reviewData`, and `pushLiveData`. Results are respectively `{acceptedIds:[...],suppressedIds:[...]}` (suppressedIds is optional), `{evaluated:1}`, and `{accepted:true}`. The SDK handles host `ping` → `pong` and `shutdown` automatically. Plugin identity comes from the host's assigned process, never from a request field supplied by the plugin.

## Tests

```sh
go test -race ./...
go build -o /tmp/daployi-plugin-probe ./testdata/probe
```

From the agent project, run `DAPLOYI_TEST_PLUGIN=/tmp/daployi-plugin-probe go test -race ./internal/plugins` to exercise the SDK against the real subprocess host, signature checks, SQLite queue, and disable lifecycle.

## Actions from dashboard controls

Register explicitly named actions **before** calling `Ready`:

```go
err := client.Action("setLight", plugin.ActionHandlerFunc(
    func(ctx context.Context, action plugin.Action) (any, error) {
        // Decode/validate action.Params, validate against the assigned config,
        // and perform your device write with ctx and a device I/O timeout.
        return map[string]any{"applied": true}, nil
    },
))
```

`ActionHandler` has an `ExecuteAction(context.Context, plugin.Action) (any, error)`
method; `ActionHandlerFunc` adapts a function to that interface. The example above
shows registration only; it does not control hardware. See
`examples/actions/main.go` for a compilable simulated light with parameter
validation, or build it with `go build ./examples/actions`.

The Deploy backend will send the action to the agent, which dispatches it to the
plugin over its existing private pipes. The plugin never receives web credentials.
`Action` contains `ID`, `Name`, JSON `Params`, and a UTC `Deadline`. Return a
JSON-serializable result when execution completes, or a `*plugin.Error` containing
a safe code/message intended for the user. Other errors and panic details are
replaced with generic messages. A successful result reports what the handler
claims; verify the physical state in your integration when that matters.

Rules and limits:

- Register at most 64 unique action names, using the identifier syntax above.
  Registration is frozen by `Ready`; duplicate names and subsequent Ready calls
  are rejected.
- Actions require the agent assignment's explicit `allowedActions` list. Empty or
  absent means no remote actions. Catalog permission tags do not grant execution.
- One handler executes at a time per plugin; another action gets `busy`. There is
  no action queue. A handler may call PublishData/ReviewData/PushLiveData while it
  runs; the IPC reader continues handling replies and ping/shutdown.
- Params and result each have a 64 KiB limit. The action deadline must be in the
  next 30 seconds. Use shorter device I/O timeouts where appropriate.
- Observe context cancellation. The agent terminates a subprocess on an action
  timeout/control disconnect to bound a stuck handler, and its normal supervisor
  may restart the plugin. It never reruns the action. The SDK itself retains the
  busy slot until the handler returns, even if cancellation was already reported.
- No action/request/result enters SQLite or the store-and-forward queue. Lost
  replies, cancellation, crashes and timeouts can leave the device outcome
  unknown. Never automatically resend a physical operation after such a result.
- Duplicate IDs are rejected for the current session through their deadline plus
  five minutes (bounded to 4,096 entries). The control session nonce changes on
  reconnect/restart, invalidating old requests. This is not an exactly-once
  guarantee at the physical device. A newly issued ID is a new intent.
- Prefer `setLight {"on":true}` over `toggleLight`. Validate writable addresses,
  ranges and values against device configuration within the plugin. Actions are
  named plugin code; they do not evaluate submitted scripts or shell commands.

### Action IPC extension (protocol v1, action version 1)

The SDK advertises `{"actionVersion":1,"actions":["setLight"]}` in `ready.params`.
An old agent ignores these extra fields. A new agent accepts legacy empty `ready`
parameters, but does not dispatch actions to that process.

Agent → plugin:

```json
{"version":1,"id":"click-123","method":"executeAction","params":{"id":"click-123","name":"setLight","params":{"on":true},"deadline":"2026-10-26T10:00:20Z"}}
```

Plugin → agent:

```json
{"version":1,"id":"click-123","result":{"on":true}}
```

Errors use the existing `{error:{code,message}}` response. The SDK also understands
`cancelAction` with the original frame ID for cooperative cancellation. The
current agent instead ends the subprocess when a dispatched action is cancelled
or times out. `shutdown` still cancels the entire SDK session.

## Plugin logs

Use `Client.Log` to send structured diagnostics to the parent agent. No stdout
printing or server credentials are needed:

```go
callCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
defer cancel()
receipt, err := client.Log(callCtx, plugin.LogInfo, "Connected to Modbus",
    map[string]any{"unitId": 1, "transport": "tcp"})
```

The fields map is optional; `client.Log(ctx, plugin.LogWarn, "Connection lost")`
is also valid. Levels are `LogTrace`, `LogDebug`, `LogInfo`, `LogWarn`, `LogError`
and `LogCritical`. The SDK assigns a random log ID and UTC timestamp. Messages
must be nonempty UTF-8 of at most 8 KiB; the optional JSON fields object is limited
to 8 KiB. Never include passwords, tokens or entire connection configurations.
There is no automatic redaction of developer-provided messages/fields.

The agent writes accepted entries through its normal structured console logger,
with trusted device ID, plugin ID, release, log ID and timestamp. Console output
respects the agent's log-level threshold. Plugin fields are nested under `fields`
and cannot override that identity. Remote forwarding is controlled separately
by the per-device assignment configuration, alongside `config`:

```json
{
  "id": "modbus",
  "release": "0.0.0-dev",
  "enabled": true,
  "logging": {"publish": false}
}
```

This is a placement illustration, not an installable Modbus manifest. Retain the
actual artifact metadata and existing valid plugin configuration. `logging` is an
agent-owned assignment setting, not a new field inside the plugin-specific
`config` object. Omitted `logging`/`publish` defaults to true. False keeps console
logging available while preventing forwarding to Deploy. Increment the manifest
revision when changing it. The SDK caller does not choose the forwarding policy.

`LogResult.Accepted` means the host admitted the entry after validation and rate
limiting. It does not mean the configured console threshold displayed it.
`LiveAccepted` means the remote event entered the current socket's live buffer;
it does **not** mean delivery or storage on the server. It is false when remote
logging is disabled, the agent is offline, the buffer is full, or a custom local
transport has no log-forwarding support. That is not an SDK error.

Logging allows ten entries per second per assigned plugin, with an initial burst
of twenty. Excess entries fail with `*plugin.Error{Code:"rate_limited"}` before
console output or remote forwarding. Do not spin/retry dropped diagnostics or
let a logging failure stop collection. Forwarded logs share the bounded live
buffer (64 messages, two-second expiry), are not persisted in SQLite, and are
never replayed after reconnect. PublishData's durable queue is unaffected.

Logs may be sent before `Ready` to explain initialization failures. The host
advertises `logVersion:1` in its initialization; calling Log with an older host
returns `unsupported_method` without sending an unknown RPC. Existing data and
action APIs remain compatible. SDK v0.0.2 does not contain Log; use this local
checkout during development or publish a new SDK tag before importing the API
from a versioned dependency.

The IPC method is `log` with `{id,timestamp,level,message,fields?}`. Its response
is `{accepted:true,liveAccepted:false}` (or true). Agent/server event details and
future viewer requirements are documented in the agent's `docs/plugins.md`.

## Agent-side change reporting

The plugin calls `PublishData` for each successful reading. The agent owns
change detection and its configuration; optional SDK discovery describes fields but never changes tolerance policy. `Ready` advertises `publishFilterVersion:1`, so suppression
receipts can be distinguished from durable acceptance.

Configure the device assignment outside the plugin-specific `config`:

```json
"publishing": {
  "defaultTolerance": 0,
  "streams": {
    "modbus.process": {
      "changeOnly": true,
      "maxSilenceSeconds": 600,
      "tolerances": {"/registers/0": 100}
    }
  }
}
```

Default tolerance applies to numeric payload values. Exact source overrides use
JSON Pointers relative to the payload; explicit zero overrides a nonzero default.
Tolerances are finite, nonnegative numbers in original published units, before
server mappings. Boolean/text/type/structural changes always qualify. Omitted
policy defaults to change-only, zero tolerance and a 600-second fresh interval.
Set `changeOnly:false` to preserve every reading, including repeated events.

A difference greater than or equal to a positive tolerance qualifies. Comparison
uses the last durably accepted full record, so small changes accumulate. Any
qualifying value queues the whole record and advances all its field baselines.
IDs and timestamps are excluded; all payload fields participate. Suppressed reads
before the fresh interval perform no SQLite operations. A failed write does not
advance the baseline; baselines survive restart. Changes to assignment policy reset
baselines. Fresh publishing uses an actual incoming successful reading, never a
synthetic cached value. Older/equal-time readings bypass reduction without moving
a newer baseline backwards.

`receipt.AcceptedIDs` means stored on the agent, not necessarily delivered.
`receipt.SuppressedIDs` means handled but deliberately not stored; do not retry
these as failures. Stable IDs are still required for uncertain transport retries.
ReviewData, PushLiveData, Log and actions remain independent.

Naming, decoding, scaling and optional display units are managed in Deploy's Field
Mapping screen. Plugins may supply names, types and units through discovery. No mandatory measurement catalog is required. Arbitrary server
JavaScript cannot be translated automatically into an agent raw tolerance. Leave
raw tolerance zero for encoded floating-point, packed-bit or multi-register data
unless the encoding and desired filtering are understood.

Publish a new SDK release before external builds import these changes. Local
projects can use a Go workspace referencing this checkout.

## Optional field discovery

A protocol plugin that can browse tags (such as a suitable EtherNet/IP or OPC UA
client) may register the fields it publishes. Plugins without discovery continue
publishing arbitrary JSON. Discovery adds nothing to Plugin Settings: Deploy
creates and displays these entries automatically in **Field Mapping**, with no
manual Save required for the initial copy mapping.

```go
if err := client.RegisterFields(ctx, plugin.DiscoveredField{
    ID: "probe-1", // Stable protocol identity; do not derive from a display name.
    Stream: "plant",
    Path: "/tags/temperature", // JSON Pointer within the published payload.
    Name: "Probe temperature",
    Type: "number",
    Unit: "°C", // Optional.
    Description: "Temperature reported by the controller", // Optional.
}); err != nil { return err }
if err := client.Ready(ctx); err != nil { return err }
```

This is a new API contract, separate from the removed tolerance-registration API.
`RegisterFields(ctx, fields...)` replaces the entire snapshot for this plugin,
across all its streams. Call sequentially before or after Ready, on startup and
when definitions change, at most once per second. Calling it without fields
withdraws all discoveries. Keep IDs stable across label/path changes. No automatic
protocol browsing is supplied by the SDK; the protocol plugin performs discovery.

Limits: 2,000 fields per plugin (matching the mapping limit), 8 MiB snapshot;
ID and stream up to 128 identifier characters, JSON Pointer up to 512 bytes, name
128 bytes, optional unit 64 bytes and description 512 bytes. Supported types:
number, integer, boolean, string, object, array. Tolerances and executable code are
not accepted in discovery. Type describes the JSON value actually published; an
exact 64-bit integer encoded as text must be described as string.

Success means the agent persisted the snapshot locally, not that Deploy has
received it. The agent forwards native `plugin.fields` messages on its existing
control WebSocket, replaying after reconnect and periodically. Definitions are not
attached to every telemetry record. Server mappings apply to records received
after the definitions; earlier raw records remain available. User edits are kept,
missing discoveries become unavailable, and user-deleted entries stay dismissed.
An agent without `fieldDiscoveryVersion:1` returns unsupported_method locally.

See `examples/discovery` for a runnable SDK example. Publishing this SDK version
and rebuilding a plugin is required to use the new method in released binaries.
