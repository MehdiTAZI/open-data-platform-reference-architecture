# AWS development composition

This root composes the first production-shaped AWS adapter for the Open Data Platform.

## Capabilities

- three-AZ VPC with public/private subnets;
- one NAT gateway per AZ when enabled;
- customer-managed KMS key with rotation;
- KMS-encrypted, versioned lakehouse S3 bucket;
- private-endpoint EKS cluster with control-plane logging;
- managed worker nodes;
- EKS Pod Identity for the `odp-data/spark` service account;
- remote OpenTofu state through the bootstrap stack.

## Bootstrap state

Create the backend once:

```bash
tofu -chdir=infrastructure/bootstrap/aws/state init
tofu -chdir=infrastructure/bootstrap/aws/state apply \
  -var='region=eu-west-1' \
  -var='state_bucket_name=<globally-unique-name>'
```

Copy `backend.hcl.example`, replace the bucket name, then initialize this root:

```bash
tofu -chdir=infrastructure/cloud/aws/dev init \
  -backend-config=backend.hcl
```

## Plan

The EKS administrator is intentionally explicit and is not inferred from the caller:

```bash
tofu -chdir=infrastructure/cloud/aws/dev plan \
  -var='platform_admin_principal_arn=arn:aws:iam::<account-id>:role/<platform-admin-role>'
```

## Security defaults

- Kubernetes API public access is disabled by default.
- Platform workloads run in private subnets.
- S3 public access is blocked and TLS is enforced.
- Lakehouse objects are encrypted with a customer-managed KMS key.
- Spark receives AWS access through EKS Pod Identity rather than static access keys.
- EKS administrator access uses the EKS Access API.

## FinOps note

The example enables one NAT gateway per availability zone for resilient private egress. NAT gateways have a material fixed and data-processing cost. For cost-sensitive non-production environments, set `enable_nat_gateways=false` only after adding the VPC endpoints/private connectivity required by the workloads.

## Terraform/OpenTofu vs GitOps boundary

This root owns cloud infrastructure and the Kubernetes foundation. Kafka, Spark runtime configuration, Airflow, Polaris, Trino, observability agents and other Kubernetes platform software remain GitOps-managed under `deployment/`.
