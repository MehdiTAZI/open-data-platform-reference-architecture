# Observability Architecture

The platform separates **telemetry transport** from **telemetry backends**.

## Contract

Workloads emit OpenTelemetry using OTLP over gRPC (`4317`) or HTTP (`4318`). This contract remains stable across environments.

```text
Platform services / data workloads
              |
             OTLP
              v
     OpenTelemetry Collector
              |
       +------+------+
       |             |
     metrics        traces/logs
       |             |
  Prometheus      pluggable backend
       |
    Grafana
```

## Standalone

The standalone profile uses:

- OpenTelemetry Collector Contrib for OTLP ingestion;
- Prometheus for metrics storage;
- Grafana OSS for visualization;
- collector debug exporter for traces until a dedicated trace backend is introduced.

Prometheus is intentionally ephemeral and retains only 24 hours locally.

## Production

Production deployments must define:

- collector gateway/agent topology and HA;
- authentication/encryption for telemetry ingress;
- cardinality and sampling controls;
- durable metrics/log/trace backends and retention;
- alert routing and SLO burn-rate alerts;
- access controls for dashboards and telemetry data;
- resource/cost budgets and multi-tenant isolation.

The platform must never use observability telemetry as a billing-grade source of truth.
