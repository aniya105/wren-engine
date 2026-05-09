import re

from sqlglot import exp
from sqlglot.dialects import Postgres as OriginalPostgres


class Vertica(OriginalPostgres):
    class Generator(OriginalPostgres.Generator):
        def order_sql(self, expression, copy=False):
            # Vertica does not support NULLS FIRST/LAST in ORDER BY
            # Strip them before generating SQL
            sql = super().order_sql(expression, copy=copy)
            sql = re.sub(r"\s+NULLS\s+(FIRST|LAST)", "", sql, flags=re.IGNORECASE)
            return sql
