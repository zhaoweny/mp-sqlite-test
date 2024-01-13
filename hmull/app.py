import copy
import logging
import multiprocessing as mp
import tomllib
from concurrent.futures import ProcessPoolExecutor, Executor, as_completed
from contextlib import ExitStack, contextmanager
from logging.config import dictConfig as logging_dictConfig
from logging.handlers import QueueListener, QueueHandler
from pathlib import Path

from more_itertools import chunked as batched
from sqlalchemy import create_engine, URL, Select, func as sa_func, text as sa_text
from sqlalchemy.orm import Session
from tqdm import tqdm

from hmull import worker
from hmull.model import ModelBase, DemoTable
from hmull.worker import WorkerConfig

logger = logging.getLogger(__name__)


class TqdmStreamHandler(logging.StreamHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.write(msg, file=self.stream)
        except Exception:  # noqa
            self.handleError(record)


def _init_db(db_url: URL) -> Session:
    engine = create_engine(db_url)
    ModelBase.metadata.drop_all(engine)
    ModelBase.metadata.create_all(engine)
    with engine.connect() as conn:
        conn.execute(sa_text("PRAGMA journal_mode=WAL"))
    return Session(engine)


class DemoApp:
    def __init__(self, root: Path, mp_context=None):
        self._stack = ExitStack()

        mp_context = mp_context or mp.get_context()
        manager: mp.Manager = self._stack.enter_context(mp_context.Manager())

        config = WorkerConfig.from_path(root, manager)
        _init_logging(config.log_path)

        self._session: Session = self._stack.enter_context(_init_db(config.db_url))
        self._stack.enter_context(_log_listener(config))
        self._executor: Executor = self._stack.enter_context(
            ProcessPoolExecutor(
                mp_context=mp_context,
                initializer=worker.init,
                initargs=(config,),
            )
        )

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self._stack.__exit__(*args, **kwargs)

    def process(self, jobs: int, ops: int = 100):
        begin = self._session.scalar(Select(sa_func.count(DemoTable.id)))
        logger.info(f"starting mp_process: op_total = {jobs * ops}")
        with tqdm(total=jobs * ops, desc="mp_process", unit="op") as pbar:
            for batch in batched(range(jobs), 200):
                # avoid generating too many jobs for executor
                for fut in as_completed(
                    self._executor.submit(worker.process, ops) for _ in batch
                ):
                    pbar.update(fut.result())
        end = self._session.scalar(Select(sa_func.count(DemoTable.id)))
        assert (end - begin) == jobs * ops


def _init_logging(log_path: Path) -> None:
    cfg_path = Path(__file__).with_name("logging.toml")
    log_cfg = tomllib.loads(cfg_path.read_text())
    for handler in log_cfg["handlers"].values():
        if handler.get("filename") is not None:
            filename = Path(handler["filename"])
            if not filename.is_absolute():
                filename = log_path / filename
            handler["filename"] = filename
    logging_dictConfig(log_cfg)


@contextmanager
def _log_listener(cfg: WorkerConfig):
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
    runtime_root = Path(__file__).parent / ".." / ".local"
    with DemoApp(runtime_root) as app:
        app.process(jobs=100, ops=100)


if __name__ == "__main__":
    mp.set_start_method("spawn")
    logger = logging.getLogger("hmull.app")
    main()
