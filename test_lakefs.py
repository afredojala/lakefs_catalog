from pyiceberg.types import NestedField, StringType, DoubleType
import pyarrow as pa
from pyiceberg.schema import Schema
import os
from lakefs_catalog import LakeFSCatalog

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
        identifier="ci.example",
        schema=schema,
        location=None
    )
except Exception:
    tbl = catalog.load_table('ci.example')

tbl.append(df)


tbl = catalog.load_table('test.example')
tbl.overwrite(df)
