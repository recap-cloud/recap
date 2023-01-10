import logging
from recap.plugins import load_analyzer_plugins
from pydantic import BaseModel, create_model
from typing import Type

log = logging.getLogger(__name__)


def load_combined_analyzer_types() -> Type[BaseModel]:
    combined_dict = {}
    plugins = load_analyzer_plugins()
    for analyzer_plugin_name, analyzer_plugin in plugins.items():
        try:
            for name, cls in analyzer_plugin.types().items():
                combined_dict |= {name: (cls, None)}
        except NotImplementedError as e:
            log.debug(
                "Skipping analyzer=%s when building analyzer types.",
                analyzer_plugin_name,
                exc_info=e,
            )
    return create_model('Metadata', **combined_dict)


Metadata = load_combined_analyzer_types()
