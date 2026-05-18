# gateway

The HelixObs gateway — an OTLP interceptor that adds entity-centric intelligence
to standard OpenTelemetry spans before forwarding them to the OTel Collector.

## What it does

Clients (helixobs Python/C++ library) send standard OTLP gRPC to the gateway on `:4317`.
For each span carrying `helix.entity.id` the gateway:

1. Reads `helix.parent.ids` (all parent entity IDs, comma-separated).
2. Looks up each parent in the server-side TraceStore → adds OTel span links.
3. If **not** an operation: registers the span in TraceStore so future children can link to it.
4. Extracts `helix.*` span events → writes to `entity_events` TimescaleDB table.
5. If `helix.entity.is_operation = "true"`: writes to `entity_operations` table (and upserts a placeholder `entities` row if the entity doesn't exist yet). Otherwise: writes to `entities` table.
6. Increments Prometheus counters and histograms.
7. Forwards the enriched span batch to the downstream OTel Collector.

Spans without `helix.entity.id` are forwarded unchanged.

## Package layout

```
cmd/
  main.go               entry point — wires all components, starts gRPC + HTTP servers
internal/
  store/
    store.go            TraceStore: entity_id → SpanRef (thread-safe, bounded FIFO)
    store_test.go
  interceptor/
    interceptor.go      core intelligence: span mutation, parent resolution, DB writes, metrics
    interceptor_test.go
  db/
    db.go               TimescaleDB writes + QueryEntityGraph (entities, entity_events, entity_operations)
  metrics/
    metrics.go          Prometheus counter/histogram registration and interface adapters
  receiver/
    receiver.go         OTLP TraceService gRPC implementation; calls interceptor then forwards
  api/
    handler.go          HTTP query API — GET /api/v1/entity/{entity_id}/graph
    handler_test.go
  notifier/
    backends.go         MessagingBackend and SCMBackend interfaces + SCMParams
    notifier.go         dispatch loop — silence check, semaphore gating, message building
    notifier_test.go
    config/
      config.go         hot-reload YAML loader; produces MessagingCall/SCMCall per event
      config_test.go
    fingerprint/
      fingerprint.go    message normalisation + SHA-256 dedup key
      fingerprint_test.go
    silence/
      silence.go        TTL-cached silence rule checker
      silence_test.go
    slack/
      slack.go          MessagingBackend: per-fingerprint rate limiting + digest
      slack_test.go
    github/
      github.go         SCMBackend: create/update/reopen GitHub Issues, body-edit only
      github_test.go
migrations/
  001_init.sql          Core schema: entities, entity_events, entity_operations hypertables
  002_instrument_memory.sql  instrument_memory table for Sherlock Tier 2 context
  003_trace_dedup.sql   operation_trace_seen dedup table
  004_sherlock_usage.sql    sherlock_usage cost ledger hypertable
  008_notifications.sql     notification_issues and notification_silences tables
instruments/
  chime-context.yml     Reference CHIME notification config (canonical, shared with Sherlock)
```

## Span attribute contract (shared with client library)

| Attribute | Type | Description |
|---|---|---|
| `helix.entity.id` | string | Entity identifier — presence triggers processing |
| `helix.instrument.id` | string | Instrument (e.g. "CHIME") |
| `helix.parent.ids` | string | Comma-separated list of ALL parent entity IDs |
| `helix.entity.is_operation` | string | `"true"` → write `entity_operations` row instead of `entities` row; do not overwrite TraceStore |

Constants are defined in `internal/interceptor/interceptor.go` (`attrEntityID`, `attrInstrumentID`, `attrParentIDs`, `attrIsOperation`). Do not rename without updating the client library.

## Span event contract

Any span event whose name starts with `helix.` is written to `entity_events`, regardless of whether the span is an entity creation or an operation.
`helix.error` additionally increments `helix_errors_total`.

## DB table routing

| Condition | DB write |
|---|---|
| `helix.entity.id` present, `helix.entity.is_operation` absent/false | `entities` (idempotent upsert) |
| `helix.entity.is_operation = "true"` | `entity_operations` + placeholder `entities` upsert if entity missing |
| Any `helix.*` span event | `entity_events` (always, regardless of entity vs operation) |

## Root entity dedup

`WriteEntity` uses `cardinality(parent_ids) > 0 OR cardinality($5::text[]) = 0` to guard
against duplicate inserts. This handles both the normal case (an existing real row is never
replaced by a placeholder) and root entities (which have empty parent arrays — PostgreSQL's
`array_length([], 1)` returns NULL, not 0, so an explicit `cardinality()` cast is required).

## HTTP query API

`GET /api/v1/entity/{entity_id}/graph` — returns a provenance DAG as node-link JSON
(`{nodes: [...], edges: [...]}`), ready for Cytoscape.js. Default `maxDepth` is 10.
The response includes ancestor nodes (recursive CTE), one level of descendants, and a
`has_error` flag on each node derived from `entity_events`.

Served on `API_ADDR` (default `:8080`), separate from the Prometheus metrics port (`:2112`).

## Notification system

The notifier dispatches Slack messages and GitHub issues for `helix.*` span events, driven
by per-instrument YAML config files loaded from `INSTRUMENTS_DIR`.

### Pluggable backends

Two interfaces in `internal/notifier/backends.go`:

- **`MessagingBackend`** — `Send(...)` + `FlushDigests(...)`. Implemented by `notifier/slack`.
- **`SCMBackend`** — `Dispatch(ctx, SCMParams)`. Implemented by `notifier/github`.

Register backends in `main.go` via `n.RegisterMessaging(name, backend)` / `n.RegisterSCM(name, backend)`.
New backends (Discord, GitLab, etc.) only need to implement the interface — no changes to core notifier logic.

### Config format (`instruments/*.yml`)

```yaml
instrument_id: CHIME
notifications:
  slack_webhook_env: CHIMEFRB_SLACK_WEBHOOK   # env var → webhook URL (instrument default)
  github_token_env:  CHIMEFRB_GITHUB_TOKEN    # env var → PAT

  events:
    helix.error:
      message_template: "{{.field}}"          # optional Go template over event Metadata
      slack:
        channel: "#chime-frb-alerts"
        webhook_env: OVERRIDE_WEBHOOK         # optional per-event override
        sample_window_seconds: 600
        max_per_window: 1
      github:
        repo: org/repo
        labels: [helixobs, bug]
        auto_close_after_days: 7
        on_recurrence_after_close: reopen     # or "new_issue"
```

Config files are hot-reloaded every `NOTIFIER_RELOAD_INTERVAL_SECS` seconds without a restart.
Credentials are resolved from environment variables at load time — never stored in YAML.

### GitHub issue lifecycle

- **New fingerprint**: creates issue with running stats in body.
- **Recurrence (open issue)**: updates body only — no new comments, no comment count growth.
- **Recurrence (closed issue, `reopen`)**: reopens with a single state-change comment, then updates body.
- **Recurrence (closed issue, `new_issue`)**: deletes DB record, creates a new issue.
- Body always shows: error summary, first/last seen, total occurrences, up to 10 recent entity IDs.

### Event flow defences

| Layer | Mechanism | Cap |
|---|---|---|
| Event channel | `chan Event` drop if full | `NOTIFIER_CHANNEL_BUFFER` (default 1000) |
| Per-backend goroutines | semaphore per backend type | 20 concurrent |
| Slack rate limiting | per-fingerprint sliding window | configurable per event |
| Silence rules | DB-cached per instrument, TTL 60s | — |

### Silence API

`POST /api/v1/silences` — create a silence rule (instrument-wide, event-type, or fingerprint).
`GET  /api/v1/silences?instrument_id=X` — list silences.
`DELETE /api/v1/silences/{id}` — remove a silence.

Silence rules are persisted in `notification_silences` and cached in-process with a 60s TTL.

## Prometheus metrics

### Pipeline throughput
| Metric | Labels | Description |
|---|---|---|
| `helix_entities_total` | `instrument_id`, `stage`, `status` | Entity spans processed |
| `helix_spans_received_total` | — | All OTLP spans received |
| `helix_spans_passthrough_total` | — | Spans forwarded without helix processing |
| `helix_span_processing_duration_seconds` | `instrument_id` | End-to-end gateway latency |

### Entity events
| Metric | Labels | Description |
|---|---|---|
| `helix_events_total` | `instrument_id`, `event_name` | All helix.* events |
| `helix_errors_total` | `instrument_id` | helix.error events |

### Parent resolution
| Metric | Labels | Description |
|---|---|---|
| `helix_parent_resolution_total` | `instrument_id`, `result` | Lookup outcomes (success/failed) |
| `helix_parent_resolution_failed_total` | `instrument_id` | Unresolved parents |
| `helix_parent_resolution_latency_seconds` | `instrument_id` | Per-parent resolution latency |

### Trace store
| Metric | Labels | Description |
|---|---|---|
| `helix_trace_store_size` | — | Current entry count |
| `helix_trace_store_hits_total` | — | Successful lookups |
| `helix_trace_store_misses_total` | — | Failed lookups |
| `helix_trace_store_evictions_total` | — | FIFO evictions |
| `helix_trace_store_lookup_duration_seconds` | — | Get() latency under mutex |

### TimescaleDB writes
| Metric | Labels | Description |
|---|---|---|
| `helix_db_writes_total` | `table`, `status` | Write attempts by table and outcome |
| `helix_db_write_duration_seconds` | `table` | Write latency per table |
| `helix_db_connections_in_use` | — | Pool connections acquired |
| `helix_db_connections_total` | — | Total pool size |

### Notifications
| Metric | Labels | Description |
|---|---|---|
| `helix_notifications_sent_total` | `instrument_id`, `type`, `event_name` | Dispatched notifications |
| `helix_notifications_suppressed_total` | `instrument_id`, `reason`, `event_name` | Rate-limited or silenced |
| `helix_notification_errors_total` | `instrument_id`, `type` | Send failures after retries |
| `helix_notification_send_duration_seconds` | `type` | Send latency per backend type |
| `helix_notification_channel_drops_total` | — | Events dropped (channel full or semaphore full) |

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `GATEWAY_ADDR` | `:4317` | gRPC listen address (OTLP receiver) |
| `COLLECTOR_ENDPOINT` | `otel-collector:4317` | Downstream OTel Collector |
| `DB_URL` | `postgres://helix:helix@db:5432/helixobs` | TimescaleDB connection string |
| `METRICS_ADDR` | `:2112` | Prometheus `/metrics` HTTP endpoint |
| `API_ADDR` | `:8080` | HTTP query API endpoint |
| `INSTRUMENTS_DIR` | `/instruments` | Directory of instrument YAML config files |
| `UI_BASE_URL` | `http://localhost:8081` | Base URL for entity inspector links in notifications |
| `GRAFANA_URL` | `http://localhost:3001` | Grafana URL for error-entities dashboard links |

## Running tests

```bash
go test ./...
```

Unit tests use a nil DB (skipped inside the interceptor) and a fresh Prometheus
registry per test — no external dependencies required.

## Adding new logic

- New span attribute extraction → `internal/interceptor/interceptor.go`
- New Prometheus metrics → `internal/metrics/metrics.go` + register in `New()`
- New DB tables/queries → `internal/db/db.go` + new migration file in `migrations/`
- New API endpoints → `internal/api/handler.go`
- New messaging backend (e.g. Discord) → implement `notifier.MessagingBackend`, register in `main.go` via `n.RegisterMessaging("discord", ...)`
- New SCM backend (e.g. GitLab) → implement `notifier.SCMBackend`, register via `n.RegisterSCM("gitlab", ...)`
- New YAML config fields → extend raw structs in `notifier/config/config.go` and update `resolveSlack`/`resolveGithub` (or add `resolveDiscord`, etc.)
- The `receiver.go` should stay thin — it only calls the interceptor and forwards
- When adding a new system attribute (like `helix.entity.is_operation`), add it to the exclusion list in `attrsToMetadata()` so it doesn't appear in the `metadata` JSONB column
