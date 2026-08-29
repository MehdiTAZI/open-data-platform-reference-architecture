# Infrastructure modules

Reusable OpenTofu modules live here. Modules must expose capability-oriented interfaces rather than leak provider-specific details into higher-level platform definitions where avoidable.

Planned core modules:

- network
- kubernetes
- object-storage
- identity/workload-identity
- secret-manager integration
- DNS/TLS integration
- observability prerequisites

Production modules require examples, input validation, outputs, version constraints and automated validation before being marked stable.
