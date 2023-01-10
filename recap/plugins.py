import logging
import typer
import sys
from .analyzers.abstract import AbstractAnalyzer
from .browsers.abstract import AbstractBrowser
from .catalogs.abstract import AbstractCatalog
from pydantic import BaseModel, create_model
from typing import Type


log = logging.getLogger(__name__)


if sys.version_info < (3, 10):
    from importlib_metadata import entry_points
else:
    from importlib.metadata import entry_points


ANALYZER_PLUGIN_GROUP = 'recap.analyzers'
BROWSER_PLUGIN_GROUP = 'recap.browsers'
CATALOG_PLUGIN_GROUP = 'recap.catalogs'
COMMAND_PLUGIN_GROUP = 'recap.commands'


def load_analyzer_plugins() -> dict[str, Type[AbstractAnalyzer]]:
    plugins = {}
    analyzer_plugins = entry_points(group=ANALYZER_PLUGIN_GROUP)
    for analyzer_plugin in analyzer_plugins:
        analyzer_plugin_name = analyzer_plugin.name
        try:
            analyzer_plugin_class = analyzer_plugin.load()
            plugins[analyzer_plugin_name] = analyzer_plugin_class
        except ImportError as e:
            log.debug(
                "Skipping analyzer=%s due to import error.",
                analyzer_plugin_name,
                exc_info=e,
            )
    return plugins


def load_browser_plugins() -> dict[str, Type[AbstractBrowser]]:
    plugins = {}
    browser_plugins = entry_points(group=BROWSER_PLUGIN_GROUP)
    for browser_plugin in browser_plugins:
        browser_plugin_name = browser_plugin.name
        try:
            browser_plugin_class = browser_plugin.load()
            plugins[browser_plugin_name] = browser_plugin_class
        except ImportError as e:
            log.debug(
                "Skipping browser=%s due to import error.",
                browser_plugin_name,
                exc_info=e,
            )
    return plugins


def load_catalog_plugins() -> dict[str, Type[AbstractCatalog]]:
    plugins = {}
    catalog_plugins = entry_points(group=CATALOG_PLUGIN_GROUP)
    for catalog_plugin in catalog_plugins:
        catalog_plugin_name = catalog_plugin.name
        try:
            catalog_plugin_class = catalog_plugin.load()
            plugins[catalog_plugin_name] = catalog_plugin_class
        except ImportError as e:
            log.debug(
                "Skipping catalog=%s due to import error.",
                catalog_plugin_name,
                exc_info=e,
            )
    return plugins


def load_command_plugins() -> dict[str, typer.Typer]:
    plugins = {}
    command_plugins = entry_points(group=COMMAND_PLUGIN_GROUP)
    for command_plugin in command_plugins:
        command_plugin_name = command_plugin.name
        try:
            command_plugin_class = command_plugin.load()
            plugins[command_plugin_name] = command_plugin_class
        except ImportError as e:
            log.debug(
                "Skipping command=%s due to import error.",
                command_plugin_name,
                exc_info=e,
            )
    return plugins


def init_command_plugins(app: typer.Typer) -> typer.Typer:
    plugins = load_command_plugins()

    for command_plugin_name, command_plugin in plugins.items():
        # If the plugin has a single command, then put it directly into the
        # current Typer app. If the plugin has multiple commands, then
        # treat it as a command group, and add it as such.
        # TODO Shouldn't need to do this, but Typer has a bug.
        # https://github.com/tiangolo/typer/issues/119
        if len(command_plugin.registered_commands) == 1:
            callback = command_plugin.registered_commands[0].callback
            app.command(command_plugin_name)(callback) # pyright: ignore [reportGeneralTypeIssues]
        else:
            app.add_typer(command_plugin, name=command_plugin_name)

    return app
