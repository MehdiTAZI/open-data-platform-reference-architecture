# Governance Architecture

Governance is represented through versioned metadata and enforceable controls rather than documentation alone.

## Minimum governed metadata

Every production data product should identify:

- owner/team and domain
- outputs and dependencies
- classification
- retention requirement
- data contract/schema
- quality expectations
- service level objectives
- lineage identifiers
- authorized consumer model

The `specs/` directory begins the machine-readable contract for these concepts. Catalog and policy integrations will consume the same specifications rather than define independent sources of truth.
