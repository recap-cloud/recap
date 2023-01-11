import sqlalchemy as sa
from pydantic import BaseModel
from recap.analyzers.db.abstract import AbstractDatabaseAnalyzer
from typing import List, Type


class TableIndex(BaseModel):
    columns: List[str]
    unique: bool


class TableIndexes(BaseModel):
    __root__: dict[str, TableIndex]


class TableIndexAnalyzer(AbstractDatabaseAnalyzer):
    @staticmethod
    def types() -> dict[str, Type[BaseModel]]:
        return {'indexes': TableIndexes}

    def analyze_table(
        self,
        schema: str,
        table: str,
        is_view: bool = False
    ) -> dict[str, TableIndexes] | None:
        indexes = {}
        index_dicts = sa.inspect(self.engine).get_indexes(table, schema)
        for index_dict in index_dicts:
            indexes[index_dict['name']] = TableIndex(
                columns=index_dict.get('column_names', []),
                unique=index_dict['unique'],
            )
        if indexes:
            return {'indexes': TableIndexes.parse_obj(indexes)}
        return None
