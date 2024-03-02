import re
from typing import (
    TYPE_CHECKING,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)

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
    update_table_metadata,
)
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER
from pyiceberg.typedef import EMPTY_DICT, Identifier, Properties

from lakefs_catalog.lakefs_table import LakeFSTable
from lakefs_catalog.metadata import new_table_metadata

DEFAULT_PROPERTIES = {"write.parquet.compression-codec": "zstd"}

if TYPE_CHECKING:
    import pyarrow as pa


class LakeFSCatalog(Catalog):
    repository: str

    def __init__(self, name: str, **properties):
        self.name = name
        keys = properties.keys()
        if (
            "s3.endpoint" not in keys
            or "s3.access-key-id" not in keys
            or "s3.secret-access-key" not in keys
        ):
            raise AttributeError("Fill Properties correct")
        self.properties = properties
        self.repository = properties["repository"]

    def _list_metadata_files(self, location: str) -> List[str]:
        pattern = f"{location}/metadata/*.json"
        s3 = self._get_filesystem()
        files = [str(f) for f in s3.glob(pattern)]
        return files

    def _get_filesystem(self) -> s3fs.S3FileSystem:
        return s3fs.S3FileSystem(
            endpoint_url=self.properties["s3.endpoint"],
            key=self.properties["s3.access-key-id"],
            secret=self.properties["s3.secret-access-key"],
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

    @staticmethod
    def _get_metadata_location(location: str, new_version: int = 0) -> str:
        if new_version < 0:
            raise ValueError(
                f"Metadata version: {new_version} must be a non-negative integer"
            )
        return f"{location}/metadata/v{new_version}.metadata.json"

    def _identifier_to_lakefs_path(
        self, identifier: Union[str, Identifier]
    ) -> Tuple[str, str]:
        if isinstance(identifier, str):
            parts = identifier.split(".")
        elif isinstance(identifier, Tuple):
            parts = identifier
        else:
            raise ValueError("identifier needs to be correct")
        branch = parts[0]

        return f"s3a://{self.repository}/{branch}", "/".join(parts[1:])

    @staticmethod
    def _parse_metadata_version(metadata_location: str) -> int:
        file_name = metadata_location.split("/")[-1]
        version_str = file_name.split(".")[0]
        version = re.sub(r"\D", "", version_str)
        return int(version)

    def create_table(
        self,
        identifier: Union[str, Identifier],
        schema: Union[Schema, "pa.Schema"],
        location: Optional[str] = None,
        partition_spec: PartitionSpec = UNPARTITIONED_PARTITION_SPEC,
        sort_order: SortOrder = UNSORTED_SORT_ORDER,
        properties: Properties = EMPTY_DICT,
    ):
        properties = {**DEFAULT_PROPERTIES, **properties}
        lakefs_path, location = self._identifier_to_lakefs_path(identifier)

        location_path = f"{lakefs_path}/{location}"
        metadata_location = self._get_metadata_location(location_path)
        metadata = new_table_metadata(
            location=location,
            schema=schema,
            partition_spec=partition_spec,
            sort_order=sort_order,
            properties=properties,
        )

        io = self._load_file_io(
            {**self.properties, **properties}, location=metadata_location
        )
        try:
            self._write_metadata(metadata, io, metadata_location)
        except FileExistsError:
            raise TableAlreadyExistsError from None

        self._update_version_hint(
            location_path, self._parse_metadata_version(metadata_location)
        )

        identifier_tuple = self.identifier_to_tuple_without_catalog(identifier)
        return LakeFSTable(
            identifier=identifier_tuple,
            metadata=metadata,
            metadata_location=metadata_location,
            io=io,
            catalog=self,
        )

    def register_table(
        self, identifier: Union[str, Identifier], metadata_location: str
    ) -> LakeFSTable:
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
        lakefs_path, location = self._identifier_to_lakefs_path(
            identifier_tuple
        )
        lakefs_location = f"{lakefs_path}/{location}"
        current_table = self.load_table(identifier_tuple)
        base_metadata = current_table.metadata
        for requirement in table_request.requirements:
            requirement.validate(base_metadata)
        updated_metadata = update_table_metadata(
            base_metadata, table_request.updates
        )
        if updated_metadata == base_metadata:
            # no changes, do nothing
            return CommitTableResponse(
                metadata=base_metadata,
                metadata_location=metadata_location,
            )

        # write new metadata
        new_metadata_version = (
            self._parse_metadata_version(current_table.metadata_location) + 1
        )
        new_metadata_location = self._get_metadata_location(
            lakefs_location, new_metadata_version
        )
        self._write_metadata(
            updated_metadata, current_table.io, new_metadata_location
        )

        self._update_version_hint(lakefs_location, new_metadata_version)

        return CommitTableResponse(
            metadata=updated_metadata, metadata_location=new_metadata_location
        )

    def load_table(self, identifier: Union[str, Identifier]) -> LakeFSTable:
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
        lakefs_path, location = self._identifier_to_lakefs_path(identifier)
        location_path = f"{lakefs_path}/{location}"

        io = self._load_file_io({**self.properties}, location=location_path)

        try:
            metadata_location = self._get_latest_metadata(location_path)
        except FileNotFoundError:
            raise NoSuchTableError(
                f"Table: {identifier} does not exists"
            ) from None

        metadata_file = io.new_input(metadata_location)

        metadata = FromInputFile.table_metadata(metadata_file)
        return LakeFSTable(
            identifier=self.identifier_to_tuple(identifier),
            metadata=metadata,
            metadata_location=metadata_location,
            io=io,
            catalog=self,
        )

    def _get_location(self, identifier: Union[str, Identifier]) -> str:
        identifier_tuple = self.identifier_to_tuple_without_catalog(identifier)
        return f"s3a://{self.repository}/{'/'.join(identifier_tuple)}"

    def drop_table(self, identifier: Union[str, Identifier]) -> None:
        raise NotImplementedError

    def purge_table(self, identifier: Union[str, Identifier]) -> None:
        raise NotImplementedError

    def rename_table(
        self,
        from_identifier: Union[str, Identifier],
        to_identifier: Union[str, Identifier],
    ) -> LakeFSTable:
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
        return dict(location=f"s3://{self.properties['repo']}/{namespace}")

    def update_namespace_properties(
        self,
        namespace: Union[str, Identifier],
        removals: Optional[Set[str]] = None,
        updates: Properties = EMPTY_DICT,
    ) -> PropertiesUpdateSummary:
        raise NotImplementedError
