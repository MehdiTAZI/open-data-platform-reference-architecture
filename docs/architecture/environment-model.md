# Environment Model

## Objectives

The platform supports the same logical capabilities across local, development, staging and production environments while allowing different scale, durability and availability settings.

| Environment | Purpose | Data | Availability | Change path |
|---|---|---|---|---|
| local | Developer integration and architecture validation | Synthetic only | Best effort | Local Git checkout |
| dev | Shared engineering validation | Synthetic/masked | Best effort | CI/CD + GitOps |
| staging | Pre-production validation | Masked/representative | Production-like | CI/CD + GitOps |
| production | Business workloads | Governed production data | Tiered SLO | Protected CI/CD + GitOps |

## Isolation

Production SHOULD use independent cloud accounts/subscriptions/projects from non-production. At minimum, separate environments must have independent identities, secrets, state, namespaces, storage locations and catalog namespaces.

## Promotion

Application artifacts are built once and promoted by immutable version. Environment-specific configuration is externalized. Rebuilding a binary/container for each environment is prohibited unless the artifact itself must differ for security/legal reasons.

```text
source -> build/test/scan -> immutable artifact
                              |-> dev configuration
                              |-> staging configuration
                              `-> production configuration
```

## Configuration hierarchy

1. platform defaults (non-secret)
2. environment configuration
3. domain/data-product configuration
4. secret references

Secrets are never committed as configuration values.
