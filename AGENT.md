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
migrations/
  001_init.sql          Core schema: entities, entity_events, entity_operations hypertables
  002_instrument_memory.sql  instrument_memory table for Sherlock Tier 2 context
  003_trace_dedup.sql   operation_trace_seen dedup table
  004_sherlock_usage.sql    sherlock_usage cost ledger hypertable
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

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `GATEWAY_ADDR` | `:4317` | gRPC listen address (OTLP receiver) |
| `COLLECTOR_ENDPOINT` | `otel-collector:4317` | Downstream OTel Collector |
| `DB_URL` | `postgres://helix:helix@db:5432/helixobs` | TimescaleDB connection string |
| `METRICS_ADDR` | `:2112` | Prometheus `/metrics` HTTP endpoint |
| `API_ADDR` | `:8080` | HTTP query API endpoint |

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
- The `receiver.go` should stay thin — it only calls the interceptor and forwards
- When adding a new system attribute (like `helix.entity.is_operation`), add it to the exclusion list in `attrsToMetadata()` so it doesn't appear in the `metadata` JSONB column
