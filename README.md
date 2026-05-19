# helixobs — gateway

[![Coverage Status](https://coveralls.io/repos/github/HelixObs/gateway/badge.svg?branch=main)](https://coveralls.io/github/HelixObs/gateway?branch=main)

OTLP interceptor that adds entity-centric intelligence to standard OpenTelemetry spans.

Clients send OTLP gRPC to the gateway on `:4317`. For each span carrying `helix.entity.id`
the gateway resolves parent provenance, writes to TimescaleDB, dispatches notifications,
and forwards the enriched batch to the downstream OTel Collector.

## Documentation

- **[AGENT.md](AGENT.md)** — full architecture, span attribute contract, HTTP API, Prometheus metrics, environment variables, DB schema, notification system
- **[Instrument Integration Guide](../helixobs/INSTRUMENT_SETUP.md)** — how to add a new instrument: YAML config, Slack/GitHub notifications, Sherlock AI context

## Running tests

```bash
go test ./...
```

Integration tests require a live TimescaleDB and are skipped when `TEST_DB_URL` is unset.
They run automatically in CI against a TimescaleDB service container.
