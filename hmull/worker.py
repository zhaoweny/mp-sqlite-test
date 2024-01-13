import logging
import uuid
from logging.handlers import QueueHandler
from multiprocessing.synchronize import Lock
from typing import Optional

from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session

from hmull.model import DemoConfig, DemoTable

_worker: Optional["_Worker"] = None
logger = logging.getLogger(__name__)


class _Worker:
    def __init__(self, config: DemoConfig):
        self._config = config
        self._engine = create_engine(config.db_url)

    def process(self, item_count: int):
        logger.debug(f"processing: item_count={item_count}")
        with Session(self._engine) as session:
            session.add_all(DemoTable(uuid=uuid.uuid4()) for _ in range(item_count))
            session.commit()


def init(cfg: DemoConfig):
    global _worker
    assert _worker is None, "worker is already initialized"
    logging.basicConfig(level=logging.DEBUG, handlers=[QueueHandler(cfg.log_queue)])

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
