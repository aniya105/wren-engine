from loguru import logger

from app.model import VerticaConnectionInfo
from app.model.data_source import DataSource
from app.model.metadata.dto import (
    Column,
    Constraint,
    ConstraintType,
    RustWrenEngineColumnType,
    Table,
    TableProperties,
)
from app.model.metadata.metadata import Metadata

# Vertica-specific type mapping
# Reference: https://docs.vertica.com/latest/en/sql-reference/data-types/
VERTICA_TYPE_MAPPING = {
    "boolean": RustWrenEngineColumnType.BOOL,
    "bool": RustWrenEngineColumnType.BOOL,
    "int": RustWrenEngineColumnType.INTEGER,
    "integer": RustWrenEngineColumnType.INTEGER,
    "bigint": RustWrenEngineColumnType.BIGINT,
    "smallint": RustWrenEngineColumnType.SMALLINT,
    "tinyint": RustWrenEngineColumnType.TINYINT,
    "float": RustWrenEngineColumnType.DOUBLE,
    "float8": RustWrenEngineColumnType.DOUBLE,
    "double precision": RustWrenEngineColumnType.DOUBLE,
    "real": RustWrenEngineColumnType.REAL,
    "float4": RustWrenEngineColumnType.REAL,
    "numeric": RustWrenEngineColumnType.DECIMAL,
    "decimal": RustWrenEngineColumnType.DECIMAL,
    "number": RustWrenEngineColumnType.DECIMAL,
    "char": RustWrenEngineColumnType.CHAR,
    "varchar": RustWrenEngineColumnType.VARCHAR,
    "long varchar": RustWrenEngineColumnType.TEXT,
    "text": RustWrenEngineColumnType.TEXT,
    "date": RustWrenEngineColumnType.DATE,
    "time": RustWrenEngineColumnType.TIME,
    "timestamp": RustWrenEngineColumnType.TIMESTAMP,
    "timestamptz": RustWrenEngineColumnType.TIMESTAMPTZ,
    "timestamp with timezone": RustWrenEngineColumnType.TIMESTAMPTZ,
    "timestamp without timezone": RustWrenEngineColumnType.TIMESTAMP,
    "interval": RustWrenEngineColumnType.INTERVAL,
    "interval day to second": RustWrenEngineColumnType.INTERVAL,
    "interval year to month": RustWrenEngineColumnType.INTERVAL,
    "binary": RustWrenEngineColumnType.BYTEA,
    "varbinary": RustWrenEngineColumnType.BYTEA,
    "long varbinary": RustWrenEngineColumnType.BYTEA,
    "bytea": RustWrenEngineColumnType.BYTEA,
    "uuid": RustWrenEngineColumnType.UUID,
    "json": RustWrenEngineColumnType.JSON,
    "jsonb": RustWrenEngineColumnType.JSON,
    "inet": RustWrenEngineColumnType.INET,
}


class VerticaMetadata(Metadata):
    def __init__(self, connection_info: VerticaConnectionInfo):
        super().__init__(connection_info)
        self.connection = DataSource.vertica.get_connection(connection_info)

    def get_table_list(self) -> list[Table]:
        sql = """
        SELECT
            t.table_schema,
            t.table_name,
            c.column_name,
            c.data_type,
            c.is_nullable,
            c.ordinal_position
        FROM v_catalog.tables t
        JOIN v_catalog.columns c
            ON t.table_schema = c.table_schema
            AND t.table_name = c.table_name
        WHERE t.table_schema NOT IN ('v_catalog', 'v_monitor', 'v_internal', 'pg_catalog')
          AND t.is_system_table = FALSE
        ORDER BY t.table_schema, t.table_name, c.ordinal_position
        """
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            cols = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()

        unique_tables = {}
        for row in rows:
            row_dict = dict(zip(cols, row))
            schema_table = self._format_vertica_compact_table_name(
                row_dict["table_schema"], row_dict["table_name"]
            )

            if schema_table not in unique_tables:
                unique_tables[schema_table] = Table(
                    name=schema_table,
                    description=None,
                    columns=[],
                    properties=TableProperties(
                        schema=row_dict["table_schema"],
                        catalog=None,
                        table=row_dict["table_name"],
                    ),
                    primaryKey="",
                )

            is_nullable = row_dict["is_nullable"]
            not_null = (
                not is_nullable
                if isinstance(is_nullable, bool)
                else is_nullable.lower() == "no"
            )
            unique_tables[schema_table].columns.append(
                Column(
                    name=row_dict["column_name"],
                    type=self._transform_vertica_column_type(row_dict["data_type"]),
                    notNull=not_null,
                    description=None,
                    properties=None,
                )
            )

        return list(unique_tables.values())

    def get_constraints(self) -> list[Constraint]:
        sql = """
        SELECT
            tc.table_name,
            kcu.column_name,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name,
            tc.constraint_name
        FROM v_catalog.table_constraints tc
        JOIN v_catalog.constraint_columns kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_name = kcu.table_name
        JOIN v_catalog.constraint_columns ccu
            ON tc.constraint_name = ccu.constraint_name
        WHERE tc.constraint_type = 'f'
        """
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            cols = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()

        constraints = []
        for row in rows:
            row_dict = dict(zip(cols, row))
            constraints.append(
                Constraint(
                    constraintName=row_dict["constraint_name"],
                    constraintTable=row_dict["table_name"],
                    constraintColumn=row_dict["column_name"],
                    constraintedTable=row_dict["foreign_table_name"],
                    constraintedColumn=row_dict["foreign_column_name"],
                    constraintType=ConstraintType.FOREIGN_KEY,
                )
            )
        return constraints

    def get_version(self) -> str:
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT version()")
            return cursor.fetchone()[0]

    def _format_vertica_compact_table_name(self, schema: str, table: str):
        return f"{schema}.{table}"

    def _transform_vertica_column_type(
        self, data_type: str
    ) -> RustWrenEngineColumnType:
        # Strip parameters like varchar(18) -> varchar
        normalized_type = data_type.lower().split("(")[0].strip()
        mapped_type = VERTICA_TYPE_MAPPING.get(
            normalized_type, RustWrenEngineColumnType.UNKNOWN
        )
        if mapped_type == RustWrenEngineColumnType.UNKNOWN:
            logger.warning(f"Unknown Vertica data type: {data_type}")
        return mapped_type
