from dataclasses import dataclass, field
from multiprocessing import Lock, Queue
from multiprocessing.managers import SyncManager
from pathlib import Path
from uuid import UUID

from sqlalchemy import URL
from sqlalchemy.orm import DeclarativeBase, MappedAsDataclass, Mapped, mapped_column


class ModelBase(DeclarativeBase, MappedAsDataclass):
    pass


class DemoTable(ModelBase):
    __tablename__ = "demo"

    id: Mapped[int] = mapped_column(primary_key=True, init=False)
    uuid: Mapped[UUID]


@dataclass
class DemoConfig:
    root: Path
    db_url: URL
    log_path: Path

    log_queue: Queue = field(default_factory=Queue)
    db_lock: Lock = field(default_factory=Lock)

    @classmethod
    def from_path(cls, root: Path) -> "DemoConfig":
        root.mkdir(parents=True, exist_ok=True)
        log_path = root / "logs"
        log_path.mkdir(parents=True, exist_ok=True)
        db_url = URL.create(drivername="sqlite", database=str(root / "demo.db"))
        return cls(root, log_path=log_path, db_url=db_url)
