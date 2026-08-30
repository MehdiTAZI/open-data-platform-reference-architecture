# Infrastructure modules

Reusable OpenTofu modules expose capability-oriented interfaces rather than leaking environment composition into reusable code.

## Implemented AWS modules

| Module | Capability |
|---|---|
| `aws/network` | multi-AZ VPC, public/private subnets, routing and optional resilient NAT egress |
| `aws/kms` | customer-managed encryption key with rotation |
| `aws/object-storage` | encrypted/versioned lakehouse S3 storage with public-access protections |
| `aws/eks` | managed Kubernetes foundation, private API default, logging and access API |
| `aws/workload-identity` | EKS Pod Identity binding between a Kubernetes service account and least-privilege IAM |

## Module contract

Modules must provide:

- `versions.tf` with explicit compatibility constraints;
- typed `variables.tf` with validation where useful;
- implementation in `main.tf`;
- stable, minimal `outputs.tf`;
- common platform tags;
- secure defaults and least-privilege IAM;
- at least one validated environment composition before being considered stable.

Provider-specific details remain inside provider-specific modules. Portability is achieved through equivalent capability contracts across AWS/Azure/GCP adapters, not by pretending the cloud primitives are identical.
