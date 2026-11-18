from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Config:
    """Runtime configuration.

    data_dir:   root of the parquet data lake (one ``date=YYYY-MM-DD`` folder per day)
    state_dir:  where the SQLite state database lives
    retry_delay: seconds before a failed erasure task is retried
    worker_poll: worker queue poll interval in seconds
    """

    data_dir: Path
    state_dir: Path
    retry_delay: float = 5.0
    worker_poll: float = 0.2

    @property
    def db_path(self) -> Path:
        return self.state_dir / "erasure.db"

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            data_dir=Path(os.environ.get("DATA_DIR", "./data")).resolve(),
            state_dir=Path(os.environ.get("STATE_DIR", "./state")).resolve(),
            retry_delay=float(os.environ.get("ERASURE_RETRY_DELAY", "5")),
            worker_poll=float(os.environ.get("ERASURE_WORKER_POLL", "0.2")),
        )
