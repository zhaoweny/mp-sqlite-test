import copy
import logging
import multiprocessing
import tomllib
from concurrent.futures import ProcessPoolExecutor, Executor
from contextlib import ExitStack, contextmanager
from logging.config import dictConfig as logging_dictConfig
from logging.handlers import QueueListener, QueueHandler
from pathlib import Path

from sqlalchemy import create_engine, URL, Select, func as sa_func, text as sa_text
from sqlalchemy.orm import Session

from hmull import worker
from hmull.model import ModelBase, DemoConfig, DemoTable

logger = logging.getLogger(__name__)


def _init_db(db_url: URL) -> Session:
    engine = create_engine(db_url)
    ModelBase.metadata.drop_all(engine)
    ModelBase.metadata.create_all(engine)
    with engine.connect() as conn:
        conn.execute(sa_text("PRAGMA journal_mode=WAL"))
    return Session(engine)


class DemoApp:
    def __init__(self, config: DemoConfig):
        self._config = config
        self._stack = ExitStack()

        self._session: Session = self._stack.enter_context(
            _init_db(self._config.db_url)
        )
        self._stack.enter_context(_log_listener(cfg=self._config))
        self._executor: Executor = self._stack.enter_context(
            ProcessPoolExecutor(initializer=worker.init, initargs=(self._config,))
        )

    def _process_actual(self, worker_func, jobs, ops):
        begin = self._session.scalar(Select(sa_func.count(DemoTable.id)))
        logger.info(f"starting mp_process: op_total = {jobs * ops}")

        for _ in self._executor.map(worker_func, [ops for _ in range(jobs)]):
            pass

        end = self._session.scalar(Select(sa_func.count(DemoTable.id)))
        assert (end - begin) == jobs * ops

    def process(self, jobs, ops=100):
        self._process_actual(worker.process, jobs, ops)


def _init_logging(cfg: DemoConfig) -> None:
    cfg_path = Path(__file__).parent / "demo_config.toml"
    log_cfg = tomllib.loads(cfg_path.read_text())["logging"]
    for handler in log_cfg["handlers"].values():
        if handler.get("filename") is not None:
            filename = Path(handler["filename"])
            if not filename.is_absolute():
                filename = cfg.log_path / filename
            handler["filename"] = filename
    logging_dictConfig(log_cfg)


@contextmanager
def _log_listener(cfg: DemoConfig):
    root_logger = logging.getLogger()
    handlers = copy.copy(root_logger.handlers)
    queue_handler = QueueHandler(cfg.log_queue)

    root_logger.addHandler(queue_handler)
    for handler in handlers:
        assert handler is not QueueHandler
        root_logger.removeHandler(handler)

    listener = QueueListener(cfg.log_queue, *handlers, respect_handler_level=True)
    try:
        listener.start()
        logger.debug("log_queue listener ready")
        yield
    finally:
        listener.stop()
        for handler in handlers:
            root_logger.addHandler(handler)
        root_logger.removeHandler(queue_handler)
        logger.debug("log_queue listener stopped")


def main():
    cfg_path = Path(__file__).parent / ".." / ".local"
    cfg = DemoConfig.from_path(cfg_path)
    _init_logging(cfg)

    app = DemoApp(cfg)
    for jobs in range(100, 1000, 100):
        app.process(jobs, jobs)


if __name__ == "__main__":
    multiprocessing.set_start_method("spawn")
    logger = logging.getLogger("hmull.app")
    main()