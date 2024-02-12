import re
from typing import (
    TYPE_CHECKING,
    List,
    Optional,
    Set,
    Union,
)
import lakefs_api
import s3fs

from pyiceberg.catalog import Catalog, PropertiesUpdateSummary
from pyiceberg.exceptions import NoSuchTableError, TableAlreadyExistsError
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.serializers import FromInputFile
from pyiceberg.table import (
    CommitTableRequest,
    CommitTableResponse,
    SortOrder,
    Table,
    update_table_metadata,
)
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER
from pyiceberg.typedef import EMPTY_DICT, Identifier, Properties
from lakefs_catalog.metadata import new_table_metadata

DEFAULT_PROPERTIES = {"write.parquet.compression-codec": "zstd"}

if TYPE_CHECKING:
    import pyarrow as pa


class LakeFSCatalog(Catalog):
    configuration: lakefs_api.Configuration

    def __init__(self, name: str, **properties):
        self.name = (name,)
        self.properties = properties
        self.configuration = lakefs_api.Configuration(
            username=properties["s3.access-key-id"],
            password=properties["s3.secret-access-key"],
            host=properties["s3.endpoint"],
        )
        self.repo = properties["repo"]
        self.client = lakefs_api.ApiClient(self.configuration)

    def _list_metadata_files(self, location: str) -> str:
        pattern = f"{location}/metadata/*.json"
        s3 = self._get_filesystem()
        return s3.glob(pattern)

    def _get_filesystem(self) -> s3fs.S3FileSystem:
        return s3fs.S3FileSystem(
            endpoint_url=self.configuration.host,
            key=self.configuration.username,
            secret=self.configuration.password,
        )

    def _get_latest_metadata(self, location):
        s3 = self._get_filesystem()
        metadata_path = f"{location}/metadata"
        with s3.open(f"{metadata_path}/version-hint.text", "r") as f:
            version = f.read()

        return f"{metadata_path}/v{version}.metadata.json"

    def _update_version_hint(self, location: str, version: int):
        s3 = self._get_filesystem()
        path = f"{location}/metadata/version-hint.text"
        with s3.open(path, "w") as f:
            f.write(str(version))

    def _get_metadata_location(
        self, location: str, new_version: int = 0
    ) -> str:
        if new_version < 0:
            raise ValueError(
                f"Metadata version: {new_version} must be a non-negative integer"
            )
        return f"{location}/metadata/v{new_version}.metadata.json"

    def _convert_to_lakefs_path(self, location) -> str:
        return f"s3a://{self.repo}/{location}"

    def _parse_metadata_version(self, metadata_location: str) -> int:
        file_name = metadata_location.split("/")[-1]
        version_str = file_name.split(".")[0]
        version = re.sub(r"\D", "", version_str)
        return int(version)

    def create_table(
        self,
        identifier: Union[str, Identifier],
        schema: Union[Schema, "pa.Schema"],
        location: Optional[str],
        partition_spec: PartitionSpec = UNPARTITIONED_PARTITION_SPEC,
        sort_order: SortOrder = UNSORTED_SORT_ORDER,
        properties: Properties = EMPTY_DICT,
    ):
        properties = {**DEFAULT_PROPERTIES, **properties}
        database_name, table_name = self.identifier_to_database_and_table(
            identifier
        )

        location = self._resolve_table_location(
            location, database_name, table_name
        )
        print(location)

        metadata_location = self._get_metadata_location(location)
        metadata = new_table_metadata(
            location=location,
            schema=schema,
            partition_spec=partition_spec,
            sort_order=sort_order,
            properties=properties,
        )

        io = self._load_file_io(
            {**self.properties, **properties}, location=location
        )
        try:
            self._write_metadata(metadata, io, metadata_location)
        except FileExistsError:
            raise TableAlreadyExistsError

        self._update_version_hint(
            location, self._parse_metadata_version(metadata_location)
        )

        identifier_tuple = self.identifier_to_tuple_without_catalog(identifier)
        return Table(
            identifier=identifier_tuple,
            metadata=metadata,
            metadata_location=metadata_location,
            io=io,
            catalog=self,
        )

    def register_table(
        self, identifier: Union[str, Identifier], metadata_location: str
    ) -> Table:
        raise NotImplementedError

    def _commit_table(
        self, table_request: CommitTableRequest
    ) -> CommitTableResponse:
        identifier_tuple = self.identifier_to_tuple_without_catalog(
            tuple(
                table_request.identifier.namespace.root
                + [table_request.identifier.name]
            )
        )
        current_table = self.load_table(identifier_tuple)
        database_name, table_name = self.identifier_to_database_and_table(
            identifier_tuple, NoSuchTableError
        )
        base_metadata = current_table.metadata
        for requirement in table_request.requirements:
            requirement.validate(base_metadata)
        print(base_metadata)
        updated_metadata = update_table_metadata(
            base_metadata, table_request.updates
        )
        print(updated_metadata)
        if updated_metadata == base_metadata:
            # no changes, do nothing
            return CommitTableResponse(
                metadata=base_metadata,
                metadata_location=current_table.metadata_location,
            )

        # write new metadata
        new_metadata_version = (
            self._parse_metadata_version(current_table.metadata_location) + 1
        )
        print(new_metadata_version)
        new_metadata_location = self._get_metadata_location(
            current_table.metadata.location, new_metadata_version
        )
        new_metadata_location = self._convert_to_lakefs_path(
            new_metadata_location)
        self._write_metadata(
            updated_metadata, current_table.io, new_metadata_location
        )

        self._update_version_hint(
            self._convert_to_lakefs_path(
                current_table.location()), new_metadata_version
        )

        return CommitTableResponse(
            metadata=updated_metadata, metadata_location=new_metadata_location
        )

    def load_table(self, identifier: Union[str, Identifier]) -> Table:
        """Load the table's metadata and returns the table instance.

        You can also use this method to check for table existence using
        'try catalog.table() except TableNotFoundError'.

        Note: This method doesn't scan data stored in the table.

        Args:
            identifier: Table identifier.

        Returns:
            Table: the table instance with its metadata.

        Raises:
            NoSuchTableError: If a table with the name does not exist,
            or the identifier is invalid.
        """
        identifier_tuple = self.identifier_to_tuple_without_catalog(identifier)
        database_name, table_name = self.identifier_to_database_and_table(
            identifier_tuple, NoSuchTableError
        )
        location = self._get_location(identifier)

        io = self._load_file_io({**self.properties}, location=location)

        metadata_location = self._get_latest_metadata(location)
        metadata_file = io.new_input(metadata_location)
        print(metadata_file.location)

        metadata = FromInputFile.table_metadata(metadata_file)
        return Table(
            identifier=identifier_tuple,
            metadata=metadata,
            metadata_location=metadata_location,
            io=io,
            catalog=self,
        )

    def _get_location(self, identifier: Union[str, Identifier]) -> str:
        identifier_tuple = self.identifier_to_tuple_without_catalog(identifier)
        return f"s3a://{self.repo}/{'/'.join(identifier_tuple)}"

    def drop_table(self, identifier: Union[str, Identifier]) -> None:
        raise NotImplementedError

    def purge_table(self, identifier: Union[str, Identifier]) -> None:
        raise NotImplementedError

    def rename_table(
        self,
        from_identifier: Union[str, Identifier],
        to_identifier: Union[str, Identifier],
    ) -> Table:
        raise NotImplementedError

    def create_namespace(
        self,
        namespace: Union[str, Identifier],
        properties: Properties = EMPTY_DICT,
    ) -> None:
        raise NotImplementedError

    def drop_namespace(self, namespace: Union[str, Identifier]) -> None:
        raise NotImplementedError

    def list_tables(
        self, namespace: Union[str, Identifier]
    ) -> List[Identifier]:
        raise NotImplementedError

    def list_namespaces(
        self, namespace: Union[str, Identifier] = ()
    ) -> List[Identifier]:
        raise NotImplementedError

    def load_namespace_properties(
        self, namespace: Union[str, Identifier]
    ) -> Properties:
        return dict(
            location=f"s3://{self.properties['repo']}/{namespace}"
        )

    def update_namespace_properties(
        self,
        namespace: Union[str, Identifier],
        removals: Optional[Set[str]] = None,
        updates: Properties = EMPTY_DICT,
    ) -> PropertiesUpdateSummary:
        raise NotImplementedError
