import logging
import sqlalchemy as sa
from abc import abstractmethod
from contextlib import contextmanager
from pathlib import PurePosixPath
from pydantic import BaseModel
from recap.analyzers.abstract import AbstractAnalyzer
from recap.browsers.db import DatabasePath
from typing import Generator


log = logging.getLogger(__name__)


class AbstractDatabaseAnalyzer(AbstractAnalyzer):
    def __init__(
        self,
        engine: sa.engine.Engine,
    ):
        self.engine = engine

    @staticmethod
    def analyzable(url: str) -> bool:
        # TODO there's probably a better way to do this.
        # Seems like SQLAlchemy should have a method to check dialects.
        try:
            sa.create_engine(url)
            return True
        except Exception as e:
            log.debug('Unanalyzable. Create engine failed for url=%s', url)
            return False

    def analyze(
        self,
        path: PurePosixPath
    ) -> dict[str, BaseModel] | None:
        database_path = DatabasePath(path)
        schema = database_path.schema
        table = database_path.table
        if schema and table:
            is_view = path.parts[3] == 'views'
            return self.analyze_table(schema, table, is_view)
        return None

    @abstractmethod
    def analyze_table(
        self,
        schema: str,
        table: str,
        is_view: bool = False,
    ) -> dict[str, BaseModel] | None:
        raise NotImplementedError

    @classmethod
    @contextmanager
    def open(cls, **config) -> Generator['AbstractDatabaseAnalyzer', None, None]:
        assert 'url' in config, \
            f"Config for {cls.__name__} is missing `url` config."
        engine = sa.create_engine(config['url'])
        yield cls(engine)
