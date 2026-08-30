# Cloud adapters

Cloud adapters implement the platform capability contracts with provider-native services while preserving the stable workload-facing interfaces described under `docs/architecture/runtime-interfaces.md`.

## AWS

The first executable production adapter lives under `aws/dev` and composes:

- VPC and private/public subnet topology;
- KMS encryption;
- S3 lakehouse storage;
- Amazon EKS;
- EKS access management;
- EKS Pod Identity for workloads.

AWS is intentionally implemented first so the repository can demonstrate one complete production-shaped path before duplicating abstractions across providers.

## Azure and GCP

Future Azure and GCP adapters should implement equivalent capabilities rather than copy AWS resource structure. The portable boundary is the platform contract, not a lowest-common-denominator Terraform module.
