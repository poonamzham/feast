import json
from typing import Callable, Dict, Iterable, Optional, Tuple

from typeguard import typechecked

from feast.data_source import DataSource
from feast.errors import DataSourceNoNameException
from feast.infra.utils.db2.connection_utils import _get_conn
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.protos.feast.core.SavedDataset_pb2 import (
    SavedDatasetStorage as SavedDatasetStorageProto,
)
from feast.repo_config import RepoConfig
from feast.saved_dataset import SavedDatasetStorage
from feast.type_map import pg_type_code_to_pg_type, pg_type_to_feast_value_type
from feast.value_type import ValueType


@typechecked
class DB2Source(DataSource):
    def __init__(
        self,
        name: Optional[str] = None,
        query: Optional[str] = None,
        table: Optional[str] = None,
        timestamp_field: Optional[str] = "",
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
        description: Optional[str] = "",
        tags: Optional[Dict[str, str]] = None,
        owner: Optional[str] = "",
    ):
        self._db2_options = DB2Options(name=name, query=query, table=table)

        # If no name, use the table as the default name.
        if name is None and table is None:
            raise DataSourceNoNameException()
        name = name or table
        assert name

        super().__init__(
            name=name,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping,
            description=description,
            tags=tags,
            owner=owner,
        )

    def __hash__(self):
        return super().__hash__()

    def __eq__(self, other):
        if not isinstance(other, DB2Source):
            raise TypeError(
                "Comparisons should only involve DB2Source class objects."
            )

        return (
            super().__eq__(other)
            and self._db2_options._query == other._db2_options._query
            and self.timestamp_field == other.timestamp_field
            and self.created_timestamp_column == other.created_timestamp_column
            and self.field_mapping == other.field_mapping
        )

    @staticmethod
    def from_proto(data_source: DataSourceProto):
        assert data_source.HasField("custom_options")

        db2_options = json.loads(data_source.custom_options.configuration)

        return DB2Source(
            name=db2_options["name"],
            query=db2_options["query"],
            table=db2_options["table"],
            field_mapping=dict(data_source.field_mapping),
            timestamp_field=data_source.timestamp_field,
            created_timestamp_column=data_source.created_timestamp_column,
            description=data_source.description,
            tags=dict(data_source.tags),
            owner=data_source.owner,
        )

    def to_proto(self) -> DataSourceProto:
        data_source_proto = DataSourceProto(
            name=self.name,
            type=DataSourceProto.CUSTOM_SOURCE,
            data_source_class_type="feast.infra.offline_stores.contrib.db2_offline_store.db2_source.DB2Source",
            field_mapping=self.field_mapping,
            custom_options=self._db2_options.to_proto(),
            description=self.description,
            tags=self.tags,
            owner=self.owner,
        )

        data_source_proto.timestamp_field = self.timestamp_field
        data_source_proto.created_timestamp_column = self.created_timestamp_column

        return data_source_proto

    def validate(self, config: RepoConfig):
        pass

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        return pg_type_to_feast_value_type

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        with _get_conn(config.offline_store) as conn, conn.cursor() as cur:
            cur.execute(
                f"SELECT * FROM ({self.get_table_query_string()}) AS sub LIMIT 0"
            )
            return (
                (c.name, pg_type_code_to_pg_type(c.type_code)) for c in cur.description
            )

    def get_table_query_string(self) -> str:

        if self._db2_options._table:
            return f"{self._db2_options._table}"
        else:
            return f"({self._db2_options._query})"


class DB2Options:
    def __init__(
        self,
        name: Optional[str],
        query: Optional[str],
        table: Optional[str],
    ):
        self._name = name or ""
        self._query = query or ""
        self._table = table or ""

    @classmethod
    def from_proto(cls, db2_options_proto: DataSourceProto.CustomSourceOptions):
        config = json.loads(db2_options_proto.configuration.decode("utf8"))
        db2_options = cls(
            name=config["name"], query=config["query"], table=config["table"]
        )

        return db2_options

    def to_proto(self) -> DataSourceProto.CustomSourceOptions:
        db2_options_proto = DataSourceProto.CustomSourceOptions(
            configuration=json.dumps(
                {"name": self._name, "query": self._query, "table": self._table}
            ).encode()
        )
        return db2_options_proto


class SavedDatasetDB2Storage(SavedDatasetStorage):
    _proto_attr_name = "custom_storage"

    db2_options: DB2Options

    def __init__(self, table_ref: str):
        self.db2_options = DB2Options(
            table=table_ref, name=None, query=None
        )

    @staticmethod
    def from_proto(storage_proto: SavedDatasetStorageProto) -> SavedDatasetStorage:
        return SavedDatasetDB2Storage(
            table_ref=DB2Options.from_proto(storage_proto.custom_storage)._table
        )

    def to_proto(self) -> SavedDatasetStorageProto:
        return SavedDatasetStorageProto(custom_storage=self.db2_options.to_proto())

    def to_data_source(self) -> DataSource:
        return DB2Source(table=self.db2_options._table)
