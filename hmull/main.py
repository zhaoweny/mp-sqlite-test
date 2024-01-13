import logging
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, Executor
from contextlib import ExitStack
from pathlib import Path

from sqlalchemy import create_engine, URL, Select, func
from sqlalchemy.orm import Session

from hmull import worker
from hmull.model import ModelBase, DemoConfig, DemoTable

logger = logging.getLogger(__name__)


def _init_db(db_url: URL) -> Session:
    engine = create_engine(db_url)
    ModelBase.metadata.drop_all(engine)
    ModelBase.metadata.create_all(engine)
    return Session(engine)


class DemoApp:
    def __init__(self, cfg_path: Path):
        self._stack = ExitStack()
        self._config = DemoConfig.from_path(cfg_path)
        worker.init_logging(self._config)

        self._session: Session = self._stack.enter_context(
            _init_db(self._config.db_url)
        )
        self._executor: Executor = self._stack.enter_context(
            ProcessPoolExecutor(initializer=worker.init, initargs=(self._config,))
        )

    def mp_process(self):
        logger.info("starting mp_process")
        for _ in self._executor.map(worker.process, [i for i in range(100)]):
            pass
        count = self._session.scalar(Select(func.count(DemoTable.id)))
        logger.info(f"total count: {count}")
        assert count is not None
        assert count == 100 * (0 + 99) / 2


def main():
    cfg_path = Path(__file__).parent / ".." / ".local"
    app = DemoApp(cfg_path)
    app.mp_process()


if __name__ == "__main__":
    multiprocessing.set_start_method("spawn")
    main()
