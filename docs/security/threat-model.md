# Platform Threat Model

## Protected assets

- Business data and derived datasets
- Credentials, keys and workload identities
- Catalog metadata, schemas, lineage and classifications
- Pipeline definitions and executable artifacts
- Infrastructure state and deployment credentials
- Audit evidence and telemetry

## Primary trust boundaries

1. Human user -> control plane
2. CI/CD -> artifact registry / deployment plane
3. Workload -> data services
4. Data platform -> source/consumer systems
5. Namespace/domain -> namespace/domain
6. Environment -> environment
7. Cloud account/subscription/project -> platform runtime

## Threats and baseline mitigations

| Threat | Baseline mitigation |
|---|---|
| Credential theft | Short-lived/federated identities; external secrets; MFA for humans |
| Privilege escalation | Least privilege; separate admin/runtime roles; policy enforcement |
| Lateral movement | NetworkPolicies, namespace isolation, segmented cloud networking |
| Malicious artifact | Protected branches, CI scanning, immutable images, provenance/signing |
| Data exfiltration | Egress controls, authorization, classification-aware policies, audit |
| Tampering | Git-reviewed declarative config, integrity-protected artifacts, audit logs |
| Accidental deletion | Object versioning where supported, backups, retention controls, tested restore |
| Tenant noisy neighbor | Resource quotas, limits, priority classes and workload isolation |
| Sensitive telemetry leakage | Redaction rules; restricted access to logs/lineage/metadata |
| Supply-chain compromise | Dependency pinning, scanning, SBOM and controlled registries |

## Explicit non-goals for local mode

The standalone environment is for development and integration validation. It MUST NOT contain production credentials or production datasets and does not claim production-grade tenant isolation.
