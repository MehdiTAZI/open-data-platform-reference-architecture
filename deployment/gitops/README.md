# GitOps Delivery Model

Argo CD is the reference GitOps reconciler for platform services.

## Rules

- CI builds, tests and publishes immutable artifacts.
- Git contains the desired deployment state and environment-specific non-secret configuration.
- GitOps reconciles clusters; CI does not normally mutate production clusters directly.
- Production changes require protected pull requests and environment approval controls.
- Rollback is performed by reconciling a known-good desired state/artifact version.
- Secrets are references to an external secret manager; plaintext secret material is prohibited in Git.

The repository will introduce Argo CD applications only after each service has explicit dependencies and health checks. This avoids creating an opaque bootstrap ordering problem.
