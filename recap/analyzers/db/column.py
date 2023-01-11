import logging
import sqlalchemy as sa
from pydantic import BaseModel
from recap.analyzers.db.abstract import AbstractDatabaseAnalyzer
from typing import Type


log = logging.getLogger(__name__)


class TableColumn(BaseModel):
    autoincrement: bool
    default: str | None
    nullable: bool
    type: str
    generic_type: str | None
    comment: str | None


class TableColumns(BaseModel):
    __root__: dict[str, TableColumn]


class TableColumnAnalyzer(AbstractDatabaseAnalyzer):
    @staticmethod
    def types() -> dict[str, Type[BaseModel]]:
        return {'columns': TableColumns}

    def analyze_table(
        self,
        schema: str,
        table: str,
        is_view: bool = False
    ) -> dict[str, TableColumns] | None:
        results = {}
        columns = sa.inspect(self.engine).get_columns(table, schema)
        for column in columns:
            try:
                generic_type = column['type'].as_generic()
                # Strip length/precision to make generic strings more generic.
                if isinstance(generic_type, sa.sql.sqltypes.String):
                    generic_type.length = None
                elif isinstance(generic_type, sa.sql.sqltypes.Numeric):
                    generic_type.precision = None
                    generic_type.scale = None
                column['generic_type'] = str(generic_type)
            except NotImplementedError as e:
                # Unable to convert. Probably a weird type like PG's OID.
                log.debug(
                    'Unable to get generic type for table=%s.%s column=%s',
                    schema,
                    table,
                    column.get('name', column),
                    exc_info=e,
                )
            # The `type` field is not JSON encodable; convert to string.
            column['type'] = str(column['type'])
            column_name = column['name']
            del column['name']
            results[column_name] = TableColumn(
                autoincrement=column['autoincrement'],
                default=column['default'],
                generic_type=column.get('generic_type'),
                nullable=column['nullable'],
                type=str(column['type']),
                comment=column.get('comment')
            )
        if results:
            return {'columns': TableColumns.parse_obj(results)}
        return None
