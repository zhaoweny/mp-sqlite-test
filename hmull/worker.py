import logging
import uuid
from dataclasses import dataclass, field
from logging.handlers import QueueHandler
from multiprocessing import Queue, Lock, RLock
import multiprocessing as mp
from pathlib import Path
from typing import Optional

from sqlalchemy import create_engine, event, URL
from sqlalchemy.orm import Session
from tqdm import tqdm

from hmull.model import DemoTable

_worker: Optional["_Worker"] = None
logger = logging.getLogger(__name__)


@dataclass
class WorkerConfig:
    db_url: URL
    db_lock: mp.Lock

    log_path: Path
    log_queue: mp.Queue

    pbar_lock: mp.RLock

    @classmethod
    def from_path(cls, root: Path, mp_manager: mp.Manager) -> "WorkerConfig":
        root.mkdir(parents=True, exist_ok=True)
        log_path = root / "logs"
        log_path.mkdir(parents=True, exist_ok=True)
        db_url = URL.create(drivername="sqlite", database=str(root / "demo.db"))
        tqdm.set_lock(mp_manager.RLock())
        return cls(
            db_url=db_url,
            db_lock=mp_manager.Lock(),
            log_path=log_path,
            log_queue=mp_manager.Queue(),
            pbar_lock=mp_manager.RLock(),
        )


class _Worker:
    def __init__(self, config: WorkerConfig):
        self._config = config
        self._engine = create_engine(config.db_url)
        tqdm.set_lock(config.pbar_lock)

    def process(self, item_count: int):
        logger.debug(f"processing: item_count={item_count}")
        with Session(self._engine) as session:
            session.add_all(DemoTable(uuid=uuid.uuid4()) for _ in range(item_count))
            session.commit()
        return item_count


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
