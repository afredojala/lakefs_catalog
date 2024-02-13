from typing import Dict, Optional
from lakefs_sdk.configuration import Configuration
from lakefs_sdk.client import LakeFSClient
from lakefs_sdk.models.commit import Commit
from lakefs_sdk.models.commit_creation import CommitCreation
from pyiceberg.typedef import Identifier
from pyiceberg.table import Table
from pyiceberg.catalog import Catalog, Properties
from pyiceberg.table.metadata import TableMetadata
from pyiceberg.io import FileIO


class LakeFSTable(Table):
    def __init__(
        self,
        identifier: Identifier,
        metadata: TableMetadata,
        metadata_location: str,
        io: FileIO,
        catalog: Catalog,
    ) -> None:
        self.catalog = catalog
        self.identifier = identifier
        self.metadata = metadata
        self.metadata_location = metadata_location
        self.io = io
        self.lakefs_client = self._create_lakefs_client(catalog.properties)

    @staticmethod
    def _create_lakefs_client(properties: Properties) -> LakeFSClient:
        configuration = Configuration(
            username=properties["s3.access-key-id"],
            password=properties["s3.secret-access-key"],
            host=properties["s3.endpoint"],
        )

        return LakeFSClient(configuration)

    def location(self) -> str:
        lakefs_location = self._get_lakefs_location()
        return f"{lakefs_location}/{self.metadata.location}"

    def _get_lakefs_location(self):
        repository = self.catalog.properties["repository"]
        branch = self.identifier[0]
        return f"s3a://{repository}/{branch}"

    def commit_lakefs(
        self, message: str, metadata: Optional[Dict[str, str]] = None
    ) -> Commit:
        branch = self.identifier[0]
        commit = CommitCreation(message=message, metadata=metadata)
        api_response = self.lakefs_client.commits_api.commit(
            repository=self.catalog.properties["repository"],
            branch=branch,
            commit_creation=commit,
        )
        return api_response
