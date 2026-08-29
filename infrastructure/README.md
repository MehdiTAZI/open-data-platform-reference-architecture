# Infrastructure as Code

OpenTofu is the reference IaC engine.

## Layering

```text
modules/       reusable provider-backed capability modules
environments/  local and shared environment composition
cloud/         AWS/Azure/GCP adapters and production compositions
```

## State

Production remote state must use encryption, locking/concurrency controls and tightly scoped access. State is treated as sensitive because provider outputs can contain infrastructure metadata or secrets.

## Module standard

Stable modules require:

- explicit provider and OpenTofu version constraints;
- typed variables with validation;
- useful outputs without unnecessary sensitive values;
- examples/tests;
- tagging/label support;
- least-privilege IAM design;
- lifecycle/upgrade documentation.
