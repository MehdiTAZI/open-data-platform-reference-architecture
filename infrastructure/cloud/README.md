# Cloud adapters

The logical architecture is cloud-neutral. This directory contains provider-specific compositions for AWS, Azure and GCP.

A cloud adapter must map the same platform capabilities (network, Kubernetes, object storage, identity, secrets, observability integration) without changing data-product or data-contract specifications.

No cloud adapter is considered production-ready until it satisfies the production readiness standard and has an automated deployment/teardown test in an isolated account/subscription/project.
