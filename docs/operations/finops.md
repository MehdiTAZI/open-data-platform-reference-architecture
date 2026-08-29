# FinOps Model

## Mandatory allocation dimensions

Production resources must be attributable to:

- environment
- platform capability
- domain
- data product/project where meaningful
- owner/team
- cost center when required by the organization

## Controls

- enforce tags/labels through IaC and policy;
- expose storage and compute consumption metrics;
- separate shared platform cost from attributable workload cost;
- use autoscaling and ephemeral compute where it improves economics without violating SLOs;
- define retention and lifecycle policies for object storage, logs and intermediate data;
- review idle capacity and expensive queries/jobs;
- surface unit economics such as cost per pipeline run, TB processed or data product where feasible.

Cost optimization must not bypass security, durability or SLO requirements.
