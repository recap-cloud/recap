from abc import ABC, abstractmethod
from contextlib import contextmanager
from pathlib import PurePosixPath
from typing import Any, Generator


class AbstractAnalyzer(ABC):
    """
    The abstract class for all analyzers. Analyzers are responsible for
    inspecting data and returning metadata.
    """

    @abstractmethod
    def analyze(self, path: PurePosixPath) -> dict[str, Any]:
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
