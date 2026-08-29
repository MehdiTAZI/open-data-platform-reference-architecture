# Service Level Objectives

SLOs are defined per service tier and consumer expectation, not per technology brand.

## Platform service SLIs

- availability / successful request ratio
- orchestration scheduling latency
- job submission success rate
- query service success and latency
- event backbone produce/consume error rate and consumer lag
- catalog operation success and latency

## Data product SLIs

- freshness
- completeness
- successful materialization ratio
- schema compatibility
- quality-rule pass rate
- consumer availability where applicable

## Alerting principle

Alerts should be tied to user-impacting symptoms and error-budget burn where possible. Component-level alerts that have no action or user impact should remain dashboard signals rather than pages.

## Example tier

A Tier 1 hourly data product might specify:

- freshness: 95% of successful publications within 60 minutes of source cutoff
- monthly availability: 99.9% for the serving interface
- RPO: <= 1 hour
- RTO: <= 4 hours

These are examples only; production owners must explicitly approve concrete objectives.
