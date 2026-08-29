# Contributing

## Change model

All non-trivial changes should be made through a branch and pull request. A change is expected to preserve the architecture-first model and the local/production parity principle.

## Required quality gates

Before opening a PR:

```bash
make doctor
make validate
```

A PR must pass automated formatting, YAML validation, shell linting, IaC validation, schema validation and security checks as those gates become available.

## Architecture decisions

Create an ADR when a change introduces or replaces a platform-wide technology, protocol, persistence model, security boundary, governance rule or operational contract. Use `docs/adr/000-template.md`.

## Production claims

Do not describe a component as production-ready merely because it deploys successfully. It must satisfy `docs/operations/production-readiness.md` and document any exceptions.

## Security

Never commit credentials, tokens, private keys, kubeconfigs, state files containing secrets or production data. Use synthetic data in examples and tests.

## Versioning

Until v1.0, schemas may evolve. Breaking changes to published specs must nevertheless be called out explicitly in PR descriptions.
