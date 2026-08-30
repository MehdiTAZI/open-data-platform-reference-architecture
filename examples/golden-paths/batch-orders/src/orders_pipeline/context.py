from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import uuid4


@dataclass(frozen=True)
class PipelineRun:
    run_id: str
    pipeline: str
    contract_version: str
    started_at: datetime

    @classmethod
    def create(cls, contract_version: str = "v1alpha2") -> "PipelineRun":
        return cls(
            run_id=str(uuid4()),
            pipeline="batch-orders",
            contract_version=contract_version,
            started_at=datetime.now(timezone.utc),
        )
