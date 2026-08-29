# Security Policy

## Reporting vulnerabilities

Do not disclose exploitable vulnerabilities, credentials or sensitive configuration in public issues. Use GitHub's private vulnerability reporting mechanism when enabled for this repository, or contact the repository owner through a private channel.

## Repository security requirements

- No plaintext production secrets.
- No static cloud credentials in workloads.
- Prefer workload identity / federated identity.
- Pin deployable images and dependencies to controlled versions; production should prefer immutable digests.
- Scan source, dependencies, containers and IaC.
- Generate and retain an SBOM for release artifacts.
- Sign release artifacts/images when the delivery pipeline is introduced.
- Enforce least privilege and namespace/network isolation.
- Treat metadata, lineage and logs as potentially sensitive.

See `docs/security/threat-model.md` for the platform threat model.
