from __future__ import annotations

import uuid
from typing import (
    Optional,
)

from pyiceberg.partitioning import (
    PartitionSpec,
    assign_fresh_partition_spec_ids,
)
from pyiceberg.schema import Schema, assign_fresh_schema_ids
from pyiceberg.table.sorting import (
    SortOrder,
    assign_fresh_sort_order_ids,
)
from pyiceberg.typedef import (
    EMPTY_DICT,
    Properties,
)
from pyiceberg.table.metadata import (
    TableMetadataV1,
    TableMetadataV2,
    TableMetadata,
)


def new_table_metadata(
    schema: Schema,
    partition_spec: PartitionSpec,
    sort_order: SortOrder,
    location: str,
    properties: Properties = EMPTY_DICT,
    table_uuid: Optional[uuid.UUID] = None,
) -> TableMetadata:
    from pyiceberg.table import TableProperties

    fresh_schema = assign_fresh_schema_ids(schema)
    fresh_partition_spec = assign_fresh_partition_spec_ids(
        partition_spec, schema, fresh_schema
    )
    fresh_sort_order = assign_fresh_sort_order_ids(
        sort_order, schema, fresh_schema
    )

    if table_uuid is None:
        table_uuid = uuid.uuid4()

    # Remove format-version so it does not get persisted
    format_version = int(
        properties.pop(
            TableProperties.FORMAT_VERSION,
            TableProperties.DEFAULT_FORMAT_VERSION,
        )
    )
    location = "/".join(location.split("/")[-2:])
    print(location)
    if format_version == 1:
        return TableMetadataV1(
            location=location,
            last_column_id=fresh_schema.highest_field_id,
            current_schema_id=fresh_schema.schema_id,
            schema=fresh_schema,
            partition_spec=[
                field.model_dump() for field in fresh_partition_spec.fields
            ],
            partition_specs=[fresh_partition_spec],
            default_spec_id=fresh_partition_spec.spec_id,
            sort_orders=[fresh_sort_order],
            default_sort_order_id=fresh_sort_order.order_id,
            properties=properties,
            last_partition_id=fresh_partition_spec.last_assigned_field_id,
            table_uuid=table_uuid,
        )

    return TableMetadataV2(
        location=location,
        schemas=[fresh_schema],
        last_column_id=fresh_schema.highest_field_id,
        current_schema_id=fresh_schema.schema_id,
        partition_specs=[fresh_partition_spec],
        default_spec_id=fresh_partition_spec.spec_id,
        sort_orders=[fresh_sort_order],
        default_sort_order_id=fresh_sort_order.order_id,
        properties=properties,
        last_partition_id=fresh_partition_spec.last_assigned_field_id,
        table_uuid=table_uuid,
    )
