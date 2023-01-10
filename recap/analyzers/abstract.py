from abc import ABC, abstractmethod
from contextlib import contextmanager
from pathlib import PurePosixPath
from pydantic import BaseModel
from typing import Generator, List, Type


class AbstractAnalyzer(ABC):
    """
    The abstract class for all analyzers. Analyzers are responsible for
    inspecting data and returning metadata.
    """

    @staticmethod
    @abstractmethod
    def analyzable(url: str) -> bool:
        """
        Determine if this analyzer can analyze data from a given infrastructure
        instance. In plain english, `analyzable` answers the question "Can
        FooAnalyzer collect metadata from
        'postgresql://user:pass@localhost/some_db'"?

        `analyzable` is called before the analyzer is instantiated. If it
        returns False, the analyzer is skipped.

        :param url: The instance URL that to be analyzed. This URL points to
            the base of the instance; it doesn't include a path to specific
            data. For example, it would look like
            "postgresql://user:pass@localhost/some_db", rather than
            "postgresql://user:pass@localhost/some_db/some_schema/some_table".

            `analyzable` will be called once, then `analyze()` will be called
            multiple times, once for each path to data.
        :returns: True if the analyzer is compatible with the given URL, else
            False.
        """

        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def types() -> dict[str, Type[BaseModel]]:
        """
        Returns a list of Pydantic model types that the analyze can return.
        These types are used when creating Recap's FastAPI.

        :returns: A list of Pydantic models that this analyzer can return.
        """

        raise NotImplementedError

    @abstractmethod
    def analyze(
        self,
        path: PurePosixPath
    ) -> dict[str, BaseModel] | None:
        """
        Analyze a path for an infrastructure instance. Only the path is
        specified because the URL for the instance is passed in via the config
        in `open()`.

        :returns: Metadata dictionary of the format {"metadata_type": Any}.
            This data gets serialized as JSON in the catalog.
        """

        raise NotImplementedError

    @staticmethod
    @contextmanager
    @abstractmethod
    def open(**config) -> Generator['AbstractAnalyzer', None, None]:
        """
        Creates and returns an analyzer using the supplied config.
        """

        raise NotImplementedError
