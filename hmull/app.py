import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import create_engine, URL
from sqlalchemy.orm import Session

from hmull.model import ModelBase


@dataclass
class DemoConfig:
    root: Path
    db_url: URL

    @classmethod
    def from_path(cls, root: Path):
        db_url = URL.create(drivername="sqlite", database=str(root / "demo.db"))
        return cls(root, db_url=db_url)


def _init_db(db_url: URL) -> Session:
    engine = create_engine(db_url)
    ModelBase.metadata.drop_all(engine)
    ModelBase.metadata.create_all(engine)
    return Session(engine)


class DemoApp:
    def __init__(self, config: DemoConfig):
        self._config = config
        self._stack = ExitStack()
        self._executor = self._stack.enter_context(
            ProcessPoolExecutor(mp_context=multiprocessing.get_context("spawn"))
        )
        self._session = self._stack.enter_context(_init_db(self._config.db_url))

    def mp_process(self):
        pass


def main():
    pass


if __name__ == "__main__":
    main()
