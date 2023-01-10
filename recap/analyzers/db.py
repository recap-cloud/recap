import logging
import sqlalchemy as sa
from .abstract import AbstractAnalyzer
from abc import abstractmethod
from contextlib import contextmanager
from pathlib import PurePosixPath
from pydantic import BaseModel, Field
from recap.browsers.db import DatabasePath, DatabaseBrowser
from typing import Any, Generator, List, Type


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
        is_view: bool = False
    ) -> dict[str, BaseModel] | None:
        raise NotImplementedError

    @classmethod
    @contextmanager
    def open(cls, **config) -> Generator['AbstractDatabaseAnalyzer', None, None]:
        assert 'url' in config, \
            f"Config for {cls.__name__} is missing `url` config."
        engine = sa.create_engine(config['url'])
        yield cls(engine)


class TableLocationModel(BaseModel):
    database: str
    instance: str
    # Schema is a reserved word in BaseModel.
    schema_: str = Field(alias='schema')
    # TODO Should validate either table or view is set and show in JSON schema.
    table: str | None = None
    view: str | None = None


class TableLocationAnalyzer(AbstractDatabaseAnalyzer):
    def __init__(
        self,
        root: PurePosixPath,
        engine: sa.engine.Engine,
    ):
        self.root = root
        self.engine = engine
        self.database = root.parts[2]
        self.instance = root.parts[4]

    @staticmethod
    def types() -> dict[str, Type[BaseModel]]:
        return {'location': TableLocationModel}

    def analyze_table(
        self,
        schema: str,
        table: str,
        is_view: bool = False
    ) -> dict[str, BaseModel] | None:
        if schema and table:
            table_or_view = 'view' if is_view else 'table'
            kwargs = {
                'database': self.database,
                'instance': self.instance,
                'schema': schema,
                table_or_view: table,
            }
            return {'location': TableLocationModel(**kwargs)}
        return None

    @classmethod
    @contextmanager
    def open(cls, **config) -> Generator['TableLocationAnalyzer', None, None]:
        assert 'url' in config, \
            f"Config for {cls.__name__} is missing `url` config."
        engine = sa.create_engine(config['url'])
        root = DatabaseBrowser.root(**config)
        yield TableLocationAnalyzer(root, engine)


class TableColumnAnalyzer(AbstractDatabaseAnalyzer):
    def analyze_table(
        self,
        schema: str,
        table: str,
        is_view: bool = False
    ) -> dict[str, Any]:
        results = {}
        columns = sa.inspect(self.engine).get_columns(table, schema)
        for column in columns:
            if column.get('comment', None) is None:
                del column['comment']
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
            results[column_name] = column
        return {'columns': results} if results else {}


class TableIndexAnalyzer(AbstractDatabaseAnalyzer):
    def analyze_table(
        self,
        schema: str,
        table: str,
        is_view: bool = False
    ) -> dict[str, Any]:
        indexes = {}
        index_dicts = sa.inspect(self.engine).get_indexes(table, schema)
        for index_dict in index_dicts:
            indexes[index_dict['name']] = {
                'columns': index_dict.get('column_names', []),
                'unique': index_dict['unique'],
            }
        return {'indexes': indexes} if indexes else {}


class TablePrimaryKeyAnalyzer(AbstractDatabaseAnalyzer):
    def analyze_table(
        self,
        schema: str,
        table: str,
        is_view: bool = False
    ) -> dict[str, Any]:
        pk_dict = sa.inspect(self.engine).get_pk_constraint(table, schema)
        return {'primary_key': pk_dict} if pk_dict else {}


class TableForeignKeyAnalyzer(AbstractDatabaseAnalyzer):
    def analyze_table(
        self,
        schema: str,
        table: str,
        is_view: bool = False
    ) -> dict[str, Any]:
        fk_dict = sa.inspect(self.engine).get_foreign_keys(table, schema)
        return {'foreign_keys': fk_dict} if fk_dict else {}


class TableViewDefinitionAnalyzer(AbstractDatabaseAnalyzer):
    def analyze_table(
        self,
        schema: str,
        table: str,
        is_view: bool = False
    ) -> dict[str, Any]:
        if not is_view:
            return {}
        # TODO sqlalchemy-bigquery doesn't work right with this API
        # https://github.com/googleapis/python-bigquery-sqlalchemy/issues/539
        if self.engine.dialect.name == 'bigquery':
            table = f"{schema}.{table}"
        def_dict = sa.inspect(self.engine).get_view_definition(table, schema)
        return {'view_definition': def_dict} if def_dict else {}


class TableCommentAnalyzer(AbstractDatabaseAnalyzer):
    def analyze_table(
        self,
        schema: str,
        table: str,
        is_view: bool = False
    ) -> dict[str, Any]:
        try:
            comment = sa.inspect(self.engine).get_table_comment(table, schema)
            comment_text = comment.get('text')
            return {'comment': comment_text} if comment_text else {}
        except NotImplementedError as e:
            log.debug(
                'Unable to get comment for table=%s.%s',
                schema,
                table,
                exc_info=e,
            )
            return {}


class TableAccessAnalyzer(AbstractDatabaseAnalyzer):
    def analyze_table(
        self,
        schema: str,
        table: str,
        is_view: bool = False
    ) -> dict[str, Any]:
        with self.engine.connect() as conn:
            results = {}
            try:
                rows = conn.execute(
                    "SELECT * FROM information_schema.role_table_grants "
                    "WHERE table_schema = %s AND table_name = %s",
                    schema,
                    table,
                )
                for row in rows.all():
                    privilege_type = row['privilege_type']
                    user_grants: dict[str, Any] = results.get(row['grantee'], {
                        'privileges': [],
                        'read': False,
                        'write': False,
                    })
                    user_grants['privileges'].append(privilege_type)
                    if privilege_type == 'SELECT':
                        user_grants['read'] = True
                    if privilege_type in ['INSERT', 'UPDATE', 'DELETE', 'TRUNCATE']:
                        user_grants['write'] = True
                    results[row['grantee']] = user_grants
            except Exception as e:
                # TODO probably need a more tightly bound exception here
                # We probably don't have access to the information_schema, so
                # skip it.
                log.debug(
                    'Unable to fetch access for table=%s.%s',
                    schema,
                    table,
                    exc_info=e,
                )
            return {'access': results} if results else {}


class TableProfileAnalyzer(AbstractDatabaseAnalyzer):
    def analyze_table(
        self,
        schema: str,
        table: str,
        is_view: bool = False
    ) -> dict[str, Any]:
        column_analyzer = TableColumnAnalyzer(self.engine)
        # TODO This is very proof-of-concept...
        # TODO ZOMG SQL injection attacks all over!
        # TODO Is db.Table().select the right way to paramaterize tables?
        columns = column_analyzer \
            .analyze_table(schema, table) \
            .get('columns', {})
        sql_col_queries = ''
        numeric_types = [
            'BIGINT', 'FLOAT', 'INT', 'INTEGER', 'NUMERIC', 'REAL', 'SMALLINT'
        ]
        date_types = [
            'DATE', 'DATETIME', 'TIMESTAMP'
        ]
        # TODO Excluding 'JSON' because PG's 'JSONB' doesn't have LENGTH()
        string_types = [
            'CHAR', 'CLOB', 'NCHAR', 'NVARCHAR', 'TEXT', 'VARCHAR'
        ]
        binary_types = [
            'BLOB', 'VARBINARY'
        ]
        stat_types = [
            'min',
            'max',
            'average',
            'sum',
            'distinct',
            'nulls',
            'zeros',
            'negatives',
            'min_length',
            'max_length',
            'empty_strings',
            'unix_epochs',
        ]

        with self.engine.connect() as conn:
            varchar_type = 'VARCHAR'

            # BigQuery doesn't havt FLOAT or VARCHAR, so use its type.
            # TODO SQLAlchemy should expose a dialect type for a generic type.
            if conn.dialect.name == 'bigquery':
                float_type = 'FLOAT64'
                varchar_type = 'STRING'
            elif conn.dialect.name == 'snowflake':
                conn.execute("ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = TRUE")

            for column_name, column in columns.items():
                generic_type = column.get('generic_type')
                quoted_column_name = f'"{column_name}"'
                if conn.dialect.name in ['bigquery', 'mysql']:
                    quoted_column_name = f'`{column_name}`'
                if generic_type in numeric_types:
                    # TODO add approx median and quantiles
                    # TODO can we use a STRUCT or something here?
                    sql_col_queries += f"""
                        , MIN({quoted_column_name}) AS min_{column_name}
                        , MAX({quoted_column_name}) AS max_{column_name}
                        , AVG({quoted_column_name}) AS average_{column_name}
                        , SUM({quoted_column_name}) AS sum_{column_name}
                        , COUNT(DISTINCT {quoted_column_name}) AS {column_name}_distinct
                        , SUM(CASE WHEN {quoted_column_name} IS NULL THEN 1 ELSE 0 END) AS nulls_{column_name}
                        , SUM(CASE WHEN {quoted_column_name} = 0 THEN 1 ELSE 0 END) AS zeros_{column_name}
                        , SUM(CASE WHEN {quoted_column_name} < 0 THEN 1 ELSE 0 END) AS negatives_{column_name}
                    """
                if generic_type in string_types:
                    sql_col_queries += f"""
                        , MIN(LENGTH({quoted_column_name})) AS min_length_{column_name}
                        , MAX(LENGTH({quoted_column_name})) AS max_length_{column_name}
                        , COUNT(DISTINCT {quoted_column_name}) AS distinct_{column_name}
                        , SUM(CASE WHEN {quoted_column_name} IS NULL THEN 1 ELSE 0 END) AS nulls_{column_name}
                        , SUM(CASE WHEN {quoted_column_name} LIKE '' THEN 1 ELSE 0 END) AS empty_strings_{column_name}
                    """
                if generic_type in binary_types:
                    sql_col_queries += f"""
                        , MIN(LENGTH({quoted_column_name})) AS min_length_{column_name}
                        , MAX(LENGTH({quoted_column_name})) AS max_length_{column_name}
                        , COUNT(DISTINCT {quoted_column_name}) AS distinct_{column_name}
                        , SUM(CASE WHEN {quoted_column_name} IS NULL THEN 1 ELSE 0 END) AS nulls_{column_name}
                    """
                if generic_type in date_types:
                    sql_col_queries += f"""
                        , CAST(MIN({quoted_column_name}) AS {varchar_type}) AS min_{column_name}
                        , CAST(MAX({quoted_column_name}) AS {varchar_type}) AS max_{column_name}
                        , COUNT(DISTINCT {quoted_column_name}) AS distinct_{column_name}
                        , SUM(CASE WHEN {quoted_column_name} IS NULL THEN 1 ELSE 0 END) AS nulls_{column_name}
                        , SUM(CASE WHEN {quoted_column_name} = TIMESTAMP '1970-01-01 00:00:00' THEN 1 ELSE 0 END) AS unix_epochs_{column_name}
                    """

            quoted_schema = f'"{schema}"'
            quoted_table = f'"{table}"'
            if conn.dialect.name in ['bigquery', 'mysql']:
                quoted_schema = f'`{schema}`'
                quoted_table = f'`{table}`'

            sql = f"""
                SELECT
                    COUNT(*) AS count {sql_col_queries}
                FROM
                    {quoted_schema}.{quoted_table} t
            """

            rows = conn.execute(sql)
            row = dict(rows.first() or {})
            results = {}

            for column_name in columns.keys():
                col_stats = results.get(column_name, {'count': row['count']})
                for stat_type in stat_types:
                    stat_name = f"{stat_type}_{column_name}"
                    if stat_name in row:
                        stat_value = row[stat_name]
                        # JSON encoder can't handle decimal.Decimal
                        import decimal
                        if isinstance(row[stat_name], decimal.Decimal):
                            stat_value = float(row[stat_name])
                        col_stats[stat_type] = stat_value
                results[column_name] = col_stats

            return {'profile': results}
