# helixobs — gateway

[![Coverage Status](https://coveralls.io/repos/github/HelixObs/gateway/badge.svg?branch=main)](https://coveralls.io/github/HelixObs/gateway?branch=main)

OTLP interceptor that adds entity-centric intelligence to standard OpenTelemetry spans.

Clients send OTLP gRPC to the gateway on `:4317`. For each span carrying `helix.entity.id`
the gateway resolves parent provenance, writes to TimescaleDB, dispatches notifications,
and forwards the enriched batch to the downstream OTel Collector.

See [AGENT.md](AGENT.md) for the full architecture and API reference.

## Running tests

```bash
go test ./...
```
