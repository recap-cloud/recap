import sqlalchemy as sa
from recap.analyzers.db.abstract import AbstractDatabaseAnalyzer
from pydantic import BaseModel
from typing import List, Type


class TableForeignKey(BaseModel):
    constrained_columns: List[str]
    referred_columns: List[str]
    referred_schema: str
    referred_table: str


class TableForeignKeys(BaseModel):
    __root__: dict[str, TableForeignKey]


class TableForeignKeyAnalyzer(AbstractDatabaseAnalyzer):
    @staticmethod
    def types() -> dict[str, Type[BaseModel]]:
        return {'foreign_keys': TableForeignKeys}

    def analyze_table(
        self,
        schema: str,
        table: str,
        is_view: bool = False
    ) -> dict[str, TableForeignKeys] | None:
        results = {}
        fks = sa.inspect(self.engine).get_foreign_keys(table, schema)
        for fk_dict in fks or []:
            results[fk_dict['name']] = TableForeignKey(
                constrained_columns=fk_dict['constrained_columns'],
                referred_columns=fk_dict['referred_columns'],
                referred_schema=fk_dict['referred_schema'],
                referred_table=fk_dict['referred_table'],
            )
        if results:
            return {'foreign_keys': TableForeignKeys.parse_obj(results)}
        return None
