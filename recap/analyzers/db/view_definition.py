import sqlalchemy as sa
from pydantic import BaseModel
from recap.analyzers.db.abstract import AbstractDatabaseAnalyzer
from typing import Type


class TableViewDefinition(BaseModel):
    __root__: str


class TableViewDefinitionAnalyzer(AbstractDatabaseAnalyzer):
    @staticmethod
    def types() -> dict[str, Type[BaseModel]]:
        return {'view_definition': TableViewDefinition}

    def analyze_table(
        self,
        schema: str,
        table: str,
        is_view: bool = False
    ) -> dict[str, TableViewDefinition] | None:
        if not is_view:
            return {}
        # TODO sqlalchemy-bigquery doesn't work right with this API
        # https://github.com/googleapis/python-bigquery-sqlalchemy/issues/539
        if self.engine.dialect.name == 'bigquery':
            table = f"{schema}.{table}"
        def_dict = sa.inspect(self.engine).get_view_definition(table, schema)
        if def_dict:
            return {'view_definition': TableViewDefinition.parse_obj(def_dict)}
        return None
