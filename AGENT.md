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
6. Increments Prometheus counters.
7. Forwards the enriched span batch to the downstream OTel Collector.

Spans without `helix.entity.id` are forwarded unchanged.

## Package layout

```
cmd/
  main.go               entry point — wires all components, starts gRPC + Prometheus servers
internal/
  store/
    store.go            TraceStore: entity_id → SpanRef (thread-safe, bounded FIFO)
    store_test.go
  interceptor/
    interceptor.go      core intelligence: span mutation, parent resolution, DB writes, metrics
    interceptor_test.go
  db/
    db.go               TimescaleDB writes (entities, entity_events, entity_operations tables)
  metrics/
    metrics.go          Prometheus counter/histogram registration
  receiver/
    receiver.go         OTLP TraceService gRPC implementation; calls interceptor then forwards
migrations/
  001_init.sql          TimescaleDB schema (entities, entity_events, entity_operations hypertables)
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

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `GATEWAY_ADDR` | `:4317` | gRPC listen address (OTLP receiver) |
| `COLLECTOR_ENDPOINT` | `otel-collector:4317` | Downstream OTel Collector |
| `DB_URL` | `postgres://helix:helix@db:5432/helixobs` | TimescaleDB connection string |
| `METRICS_ADDR` | `:2112` | Prometheus `/metrics` HTTP endpoint |

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
- The `receiver.go` should stay thin — it only calls the interceptor and forwards
- When adding a new system attribute (like `helix.entity.is_operation`), add it to the exclusion list in `attrsToMetadata()` so it doesn't appear in the `metadata` JSONB column
