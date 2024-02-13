from lakefs_sdk.client import LakeFSClient
from lakefs_sdk.configuration import Configuration
from lakefs_sdk.api.commits_api import CommitsApi
from lakefs_sdk.models.branch_creation import BranchCreation
from lakefs_sdk.models.commit_creation import CommitCreation
from pyiceberg.types import NestedField, StringType, DoubleType
import pyarrow as pa
from pyiceberg.schema import Schema
import os
from lakefs_catalog import LakeFSCatalog

import polars as pl



catalog = LakeFSCatalog(
    "afo",
    **{
        "s3.endpoint": "http://localhost:8000",
        "s3.access-key-id": os.getenv("LAKEFS_KEY"),
        "s3.secret-access-key": os.getenv("LAKEFS_SECRET"),
        "repository": 'afo'},
)

config = Configuration(
    host='http://localhost:8000',
    username=os.getenv("LAKEFS_KEY"),
    password=os.getenv("LAKEFS_SECRET")
)
config.verify_ssl = False


data = pa.Table.from_pylist(
    [
        {"city": "Amsterdam", "lat": 52.371807, "long": 4.896029},
        {"city": "San Francisco", "lat": 37.773972, "long": -122.431297},
        {"city": "Drachten", "lat": 53.11254, "long": 6.0989},
        {"city": "Paris", "lat": 48.864716, "long": 2.349014},
    ],
)

schema = Schema(
    NestedField(1, "city", StringType(), required=False),
    NestedField(2, "lat", DoubleType(), required=False),
    NestedField(3, "long", DoubleType(), required=False),
)
try:
    tbl = catalog.create_table(
        identifier="ci.do_something",
        schema=schema,
        location=None
    )
except Exception:
    tbl = catalog.load_table('ci.do_something')

tbl.append(data)

print("Committing")
print(tbl.commit_lakefs("Testing"))
df = pl.scan_iceberg(tbl)
print(df.count().collect())
client = LakeFSClient(config)
branch_creation = BranchCreation(name="working", source='ci')
api_resp = client.branches_api.create_branch('afo', branch_creation)

tbl_branched = catalog.load_table('working.do_something')
tbl_branched.overwrite(data)
tbl_branched.commit_lakefs("Overwriting all data")

df_branched = pl.scan_iceberg(tbl_branched)
print(df_branched.count().collect())
