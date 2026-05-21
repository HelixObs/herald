# helixobs — herald

[![Coverage Status](https://img.shields.io/coveralls/github/HelixObs/herald/main)](https://coveralls.io/github/HelixObs/herald?branch=main)

OTLP interceptor that adds entity-centric intelligence to standard OpenTelemetry spans.

Clients send OTLP gRPC to the herald on `:4317`. For each span carrying `helix.entity.id`
the herald resolves parent provenance, writes to TimescaleDB, dispatches notifications,
and forwards the enriched batch to the downstream OTel Collector.

## Documentation

- **[helixobs.github.io](https://helixobs.github.io)** — full documentation site: operator guides, client API reference, dashboards
- **[AGENT.md](AGENT.md)** — full architecture, span attribute contract, HTTP API, Prometheus metrics, environment variables, DB schema, notification system
- **[Instrument Integration Guide](https://github.com/HelixObs/helixobs/blob/main/INSTRUMENT_SETUP.md)** — how to add a new instrument: YAML config, Slack/GitHub notifications, Sherlock AI context

## Running tests

```bash
go test ./...
```

Integration tests require a live TimescaleDB and are skipped when `TEST_DB_URL` is unset.
They run automatically in CI against a TimescaleDB service container.
