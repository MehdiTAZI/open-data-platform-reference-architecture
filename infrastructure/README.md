# Infrastructure as Code

OpenTofu is the reference IaC engine. The IaC layer owns cloud infrastructure and the Kubernetes foundation; Kubernetes platform software is delivered separately through GitOps.

## Layout

```text
infrastructure/
├── bootstrap/
│   └── aws/state/              # remote-state bucket and locking
├── modules/
│   └── aws/
│       ├── network/
│       ├── kms/
│       ├── object-storage/
│       ├── eks/
│       └── workload-identity/
├── cloud/
│   └── aws/dev/                # executable AWS environment composition
└── environments/
    └── local/                  # Kind-based standalone environment
```

## State

Production remote state uses encryption, versioning, locking/concurrency controls and tightly scoped access. State is treated as sensitive because provider outputs can contain infrastructure metadata or secrets.

`bootstrap/aws/state` creates the backend independently so the platform roots can then consume it through partial backend configuration.

## Module standard

Stable modules require:

- explicit provider and OpenTofu version constraints;
- typed variables with validation;
- useful outputs without unnecessary sensitive values;
- tagging/label support;
- least-privilege IAM design;
- secure defaults;
- examples/tests or executable compositions;
- lifecycle/upgrade documentation.

## Delivery boundary

OpenTofu owns resources such as VPC, subnets, EKS, IAM, KMS, S3, DNS and cloud prerequisites. GitOps owns platform applications running on Kubernetes. This avoids two control planes competing for the same Kubernetes resources.

## CI

`.github/workflows/iac.yml` runs formatting, provider initialization without a backend and static validation for all implemented modules and AWS roots.
