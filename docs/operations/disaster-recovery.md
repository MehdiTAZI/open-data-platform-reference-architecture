# Disaster Recovery Strategy

## Principle

Stateless platform components are recreated from code. Stateful components are restored from protected data/metadata according to explicit RPO/RTO.

## State inventory

Every production deployment must classify state for at least:

- object storage and lakehouse data
- catalog metadata
- orchestration metadata database
- streaming metadata/data according to retention requirements
- secrets/identity configuration references
- Git repositories and deployment manifests
- observability configuration and critical audit evidence

## Required controls

1. infrastructure can be recreated from IaC;
2. application configuration can be reconciled from Git;
3. stateful services have documented backup mechanisms;
4. backups are stored in an independent failure domain appropriate to the threat model;
5. restore is automated where practical;
6. restore tests are executed on a schedule appropriate to service tier;
7. recovery dependencies and order are documented.

## Recovery order (reference)

1. foundational cloud/network/identity
2. Kubernetes/runtime
3. secret and certificate integration
4. stateful metadata services/catalog
5. streaming/storage services
6. compute/orchestration/serving
7. data products
8. validation and controlled reopening to consumers
