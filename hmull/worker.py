import logging
import os
import random
import time
import uuid
from contextlib import ExitStack
from typing import Optional

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from hmull.model import DemoConfig, DemoTable

_worker: Optional["_Worker"] = None
logger = logging.getLogger(__name__)


class _Worker:
    def __init__(self, config: DemoConfig):
        self._config = config
        self._engine = create_engine(config.db_url)

    def process(self, item_count: int):
        with ExitStack() as stack:
            session: Session = stack.enter_context(Session(self._engine))
            time.sleep(random.random() + 1.0)
            session.add_all(DemoTable(uuid=uuid.uuid4()) for _ in range(item_count))

            self._config.db_lock
            stack.enter_context(self._config.db_lock)
            logger.info("worker gained db_lock")
            session.commit()
            return item_count


def init_logging(config: DemoConfig):
    logging.basicConfig(
        level=logging.DEBUG,
        style="{",
        format="[{asctime}] [{processName}.{name}] [{filename}:{lineno}] {levelname}: {message}",
        filename=config.log_path / f"{os.getpid()}.log",
    )


def init(cfg: DemoConfig):
    init_logging(cfg)
    logger.info("initializing worker")
    global _worker
    assert _worker is None
    _worker = _Worker(cfg)


def process(item_count: int):
    logger.info("worker start processing")
    global _worker
    assert _worker is not None
    return _worker.process(item_count)
