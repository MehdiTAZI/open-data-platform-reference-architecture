# Platform Capability Implementations

This directory contains the executable reference implementations of the capability model.

Planned layout:

```text
platform/
  ingestion/
  messaging/
  storage/
  processing/
  orchestration/
  transformation/
  serving/
  governance/
  security/
  observability/
```

Each capability implementation must provide:

1. a capability README independent of vendor jargon;
2. the selected reference implementation and version policy;
3. local deployment configuration;
4. production requirements/differences;
5. configuration interface;
6. network and identity requirements;
7. health/observability contract;
8. tests;
9. upgrade/rollback notes;
10. known limitations.
