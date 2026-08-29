# Architecture Principles

## Platform as a product

The Data Platform is an internal product with explicit users, supported golden paths, service expectations and lifecycle ownership. It is not a collection of independently installed data technologies.

## Separate control plane and data plane

The **control plane** manages specifications, policy, deployment, identity, metadata and lifecycle. The **data plane** executes workloads and stores/transports data. Production designs SHOULD isolate failure domains and permissions between both planes.

## Decouple storage, compute and metadata

Object storage, open table formats, catalogs and compute engines are separate concerns. This allows independent scaling, replacement and lifecycle management.

## Immutable delivery

Production changes flow from source control through validated build artifacts and declarative deployment. Direct manual mutation of production is an exception requiring auditable break-glass procedures.

## Workload identity

Human credentials must not be reused by workloads. Runtime authentication should use short-lived workload identities wherever the target platform supports them.

## Environment parity

Local, development and production environments expose compatible contracts. Local environments may reduce replicas, resources, persistence and external integrations, but must not introduce a fundamentally different architecture.

## Failure is designed

Every stateful capability requires explicit failure, backup, recovery and upgrade models. Every user-facing platform capability requires measurable service objectives.
