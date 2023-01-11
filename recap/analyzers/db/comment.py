import logging
import sqlalchemy as sa
from pydantic import BaseModel
from recap.analyzers.db.abstract import AbstractDatabaseAnalyzer
from typing import Type


log = logging.getLogger(__name__)


class TableComment(BaseModel):
    __root__: str


class TableCommentAnalyzer(AbstractDatabaseAnalyzer):
    @staticmethod
    def types() -> dict[str, Type[BaseModel]]:
        return {'comment': TableComment}

    def analyze_table(
        self,
        schema: str,
        table: str,
        is_view: bool = False
    ) -> dict[str, TableComment] | None:
        try:
            comment = sa.inspect(self.engine).get_table_comment(table, schema)
            comment_text = comment.get('text')
            if comment_text:
                return {'comment': TableComment.parse_obj(comment_text)}
        except NotImplementedError as e:
            log.debug(
                'Unable to get comment for table=%s.%s',
                schema,
                table,
                exc_info=e,
            )
        return None
