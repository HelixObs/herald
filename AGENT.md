# gateway

The HelixObs gateway — an OTLP interceptor that adds entity-centric intelligence
to standard OpenTelemetry spans before forwarding them to the OTel Collector.

## What it does

Clients (helixobs Python/C++ library) send standard OTLP gRPC to the gateway on `:4317`.
For each span carrying `helix.entity.id` the gateway:

1. Reads `helix.parent.ids` (all parent entity IDs, comma-separated).
2. Looks up each parent in the server-side TraceStore → adds OTel span links.
3. Registers the current span so future children can link to it.
4. Extracts `helix.*` span events → writes to `entity_events` TimescaleDB table.
5. Writes the entity record to the `entities` TimescaleDB table.
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
    db.go               TimescaleDB writes (entities + entity_events tables)
  metrics/
    metrics.go          Prometheus counter/histogram registration
  receiver/
    receiver.go         OTLP TraceService gRPC implementation; calls interceptor then forwards
migrations/
  001_init.sql          TimescaleDB schema (entities + entity_events hypertables)
```

## Span attribute contract (shared with client library)

| Attribute | Type | Description |
|---|---|---|
| `helix.entity.id` | string | Entity identifier — presence triggers processing |
| `helix.instrument.id` | string | Instrument (e.g. "CHIME") |
| `helix.parent.ids` | string | Comma-separated list of ALL parent entity IDs |

## Span event contract

Any span event whose name starts with `helix.` is written to `entity_events`.
`helix.error` additionally increments `helix_errors_total`.

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
- New DB tables/queries → `internal/db/db.go` + new migration file
- The `receiver.go` should stay thin — it only calls the interceptor and forwards
