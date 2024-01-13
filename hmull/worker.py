import logging
import uuid
from dataclasses import dataclass, field
from logging.handlers import QueueHandler
from multiprocessing import Queue as MpQueue, Lock as MpLock
from multiprocessing.synchronize import Lock
from multiprocessing.queues import Queue
from pathlib import Path
from typing import Optional

from sqlalchemy import create_engine, event, URL
from sqlalchemy.orm import Session

from hmull.model import DemoTable

_worker: Optional["_Worker"] = None
logger = logging.getLogger(__name__)


@dataclass
class WorkerConfig:
    db_url: URL
    log_path: Path

    log_queue: Queue = field(default_factory=MpQueue)
    db_lock: Lock = field(default_factory=MpLock)

    @classmethod
    def from_path(cls, root: Path) -> "WorkerConfig":
        root.mkdir(parents=True, exist_ok=True)
        log_path = root / "logs"
        log_path.mkdir(parents=True, exist_ok=True)
        db_url = URL.create(drivername="sqlite", database=str(root / "demo.db"))
        return cls(log_path=log_path, db_url=db_url)


class _Worker:
    def __init__(self, config: WorkerConfig):
        self._config = config
        self._engine = create_engine(config.db_url)

    def process(self, item_count: int):
        logger.debug(f"processing: item_count={item_count}")
        with Session(self._engine) as session:
            session.add_all(DemoTable(uuid=uuid.uuid4()) for _ in range(item_count))
            session.commit()


def init(cfg: WorkerConfig):
    global _worker
    assert _worker is None, "worker is already initialized"
    logging.basicConfig(
        level=logging.DEBUG,
        handlers=[QueueHandler(cfg.log_queue)],
        format="{message}",
        style="{",
    )

    _worker = _Worker(cfg)
    db_lock: Lock = cfg.db_lock

    @event.listens_for(Session, "before_flush")
    def _before_flush(*args, **kwargs):
        db_lock.acquire()

    @event.listens_for(Session, "after_flush")
    def _after_flush(*args, **kwargs):
        db_lock.release()

    logger.debug("worker initialized")


def process(item_count: int):
    return _worker.process(item_count)
