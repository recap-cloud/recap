import sqlalchemy as sa
from pydantic import BaseModel
from recap.analyzers.db.abstract import AbstractDatabaseAnalyzer
from typing import List, Type


class TablePrimaryKey(BaseModel):
    name: str
    constrained_columns: List[str]


class TablePrimaryKeyAnalyzer(AbstractDatabaseAnalyzer):
    @staticmethod
    def types() -> dict[str, Type[BaseModel]]:
        return {'primary_key': TablePrimaryKey}

    def analyze_table(
        self,
        schema: str,
        table: str,
        is_view: bool = False
    ) -> dict[str, TablePrimaryKey] | None:
        pk_dict = sa.inspect(self.engine).get_pk_constraint(table, schema)
        if pk_dict and pk_dict.get('name'):
            return {'primary_key': TablePrimaryKey.parse_obj(pk_dict)}
        return None
