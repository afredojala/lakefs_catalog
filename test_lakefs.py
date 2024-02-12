from pyiceberg.types import NestedField, StringType, DoubleType
from pyiceberg.catalog import load_catalog
import pyarrow as pa
from pyiceberg.transforms import IdentityTransform
from pyiceberg.table.sorting import SortOrder, SortField
from pyiceberg.transforms import DayTransform
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.types import (
    TimestampType,
    FloatType,
    DoubleType,
    StringType,
    NestedField,
    StructType,
)
from pyiceberg.schema import Schema
import os
from lakefs_catalog import LakeFSCatalog
# import lakefs_api
# from lakefs_api.rest import ApiException

# configuration = lakefs_api.Configuration(
#     username=os.getenv("LAKEFS_KEY"),
#     password=os.getenv("LAKEFS_SECRET"),
#     host='http://localhost:8000/api/v1'
# )
#
# with lakefs_api.ApiClient(configuration) as api_client:
#     # Create an instance of the API class
#     repository = 'afo'  # str |
#
#     api_instance = lakefs_api.BranchesApi(api_client)
#     branch_creation = lakefs_api.BranchCreation(name='test', source='main')
#     try:
#         api_response = api_instance.create_branch(
#             repository, branch_creation)
#     except ApiException as e:
#         print("Exception when calling ActionsApi->get_run: %s\n" % e)

catalog = LakeFSCatalog(
    "afo",
    **{
        "s3.endpoint": "http://localhost:8000",
        "s3.access-key-id": os.getenv("LAKEFS_KEY"),
        "s3.secret-access-key": os.getenv("LAKEFS_SECRET"),
        "repo": 'afo'},
)

df = pa.Table.from_pylist(
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
        identifier="ci.test",
        schema=schema,
        location="s3://afo/ci/test",
    )
except Exception:
    tbl = catalog.load_table('ci.test')

tbl.append(df)
