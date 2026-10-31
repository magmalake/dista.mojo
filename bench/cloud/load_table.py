#!/usr/bin/env python3
"""Build the cloud benchmark's table: NYC yellow taxi trips for a range of
years, as Iceberg v2 in object storage.

taxibench.example's loader/load_table.py, pointed at S3 and at more years. It
keeps that loader's rules, so the table is the one taxibench queries, only
bigger: PyIceberg writes it with its default layout, every month is cast to
one schema (the field ids are the readers' contract), each month is its own
append, and rows dated outside their file's month are dropped.

TLC republished every year from 2011 in today's schema, zone ids included;
2009 and 2010 still spell the columns differently, so the default range
starts at 2011.

The catalog is a SQLite file in a scratch directory and is thrown away: the
readers take the metadata location this prints, not a catalog.

Usage: load_table.py <s3://bucket/prefix> [--years 2011-2024] [--scratch DIR]
"""

import argparse
import concurrent.futures
import datetime as dt
import os
import sys
import tempfile
import time
import urllib.request

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import MonthTransform
from pyiceberg.types import (
    DoubleType,
    IntegerType,
    NestedField,
    StringType,
    TimestampType,
)

TLC = "https://d37ci6vzurychx.cloudfront.net/trip-data"

SCHEMA = Schema(
    NestedField(1, "VendorID", IntegerType(), required=False),
    NestedField(2, "tpep_pickup_datetime", TimestampType(), required=False),
    NestedField(3, "tpep_dropoff_datetime", TimestampType(), required=False),
    NestedField(4, "passenger_count", IntegerType(), required=False),
    NestedField(5, "trip_distance", DoubleType(), required=False),
    NestedField(6, "RatecodeID", IntegerType(), required=False),
    NestedField(7, "store_and_fwd_flag", StringType(), required=False),
    NestedField(8, "PULocationID", IntegerType(), required=False),
    NestedField(9, "DOLocationID", IntegerType(), required=False),
    NestedField(10, "payment_type", IntegerType(), required=False),
    NestedField(11, "fare_amount", DoubleType(), required=False),
    NestedField(12, "extra", DoubleType(), required=False),
    NestedField(13, "mta_tax", DoubleType(), required=False),
    NestedField(14, "tip_amount", DoubleType(), required=False),
    NestedField(15, "tolls_amount", DoubleType(), required=False),
    NestedField(16, "improvement_surcharge", DoubleType(), required=False),
    NestedField(17, "total_amount", DoubleType(), required=False),
    NestedField(18, "congestion_surcharge", DoubleType(), required=False),
    NestedField(19, "airport_fee", DoubleType(), required=False),
)

ARROW_SCHEMA = pa.schema(
    [
        pa.field("VendorID", pa.int32()),
        pa.field("tpep_pickup_datetime", pa.timestamp("us")),
        pa.field("tpep_dropoff_datetime", pa.timestamp("us")),
        pa.field("passenger_count", pa.int32()),
        pa.field("trip_distance", pa.float64()),
        pa.field("RatecodeID", pa.int32()),
        pa.field("store_and_fwd_flag", pa.string()),
        pa.field("PULocationID", pa.int32()),
        pa.field("DOLocationID", pa.int32()),
        pa.field("payment_type", pa.int32()),
        pa.field("fare_amount", pa.float64()),
        pa.field("extra", pa.float64()),
        pa.field("mta_tax", pa.float64()),
        pa.field("tip_amount", pa.float64()),
        pa.field("tolls_amount", pa.float64()),
        pa.field("improvement_surcharge", pa.float64()),
        pa.field("total_amount", pa.float64()),
        pa.field("congestion_surcharge", pa.float64()),
        pa.field("airport_fee", pa.float64()),
    ]
)


def normalise(table: pa.Table) -> pa.Table:
    """Bring one TLC month onto ARROW_SCHEMA, whichever spelling it arrived in."""
    names = {n.lower(): n for n in table.column_names}
    columns = []
    for field in ARROW_SCHEMA:
        source = names.get(field.name.lower())
        if source is None:
            columns.append(pa.nulls(table.num_rows, type=field.type))
            continue
        column = table.column(source)
        if column.type != field.type:
            column = column.cast(field.type, safe=False)
        columns.append(column)
    return pa.Table.from_arrays(columns, schema=ARROW_SCHEMA)


def in_month(table: pa.Table, year: int, month: int) -> tuple[pa.Table, int]:
    """Drop rows whose pickup falls outside the month the file is named for."""
    lo = dt.datetime(year, month, 1)
    hi = dt.datetime(year + (month == 12), month % 12 + 1, 1)
    pickup = table.column("tpep_pickup_datetime")
    keep = pc.and_(
        pc.greater_equal(pickup, pa.scalar(lo, type=pa.timestamp("us"))),
        pc.less(pickup, pa.scalar(hi, type=pa.timestamp("us"))),
    )
    keep = pc.fill_null(keep, False)
    filtered = table.filter(keep)
    return filtered, table.num_rows - filtered.num_rows


def s3_properties() -> dict:
    env = os.environ.get
    props = {}
    for var, prop in [
        ("AWS_ACCESS_KEY_ID", "s3.access-key-id"),
        ("AWS_SECRET_ACCESS_KEY", "s3.secret-access-key"),
        ("AWS_SESSION_TOKEN", "s3.session-token"),
    ]:
        if env(var):
            props[prop] = env(var)
    region = env("AWS_REGION") or env("AWS_DEFAULT_REGION")
    if region:
        props["s3.region"] = region
    if env("AWS_ENDPOINT_URL"):
        props["s3.endpoint"] = env("AWS_ENDPOINT_URL")
        props["s3.force-virtual-addressing"] = "false"
    return props


def download(scratch: str, year: int, month: int) -> str:
    name = f"yellow_tripdata_{year}-{month:02d}.parquet"
    path = os.path.join(scratch, name)
    if not os.path.exists(path):
        urllib.request.urlretrieve(f"{TLC}/{name}", path + ".part")
        os.rename(path + ".part", path)
    return path


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("warehouse", help="s3://bucket/prefix")
    ap.add_argument("--years", default="2011-2024")
    ap.add_argument("--months", type=int, default=12, help="months per year")
    ap.add_argument("--scratch", default=tempfile.gettempdir())
    args = ap.parse_args()

    first, _, last = args.years.partition("-")
    months = [
        (y, m)
        for y in range(int(first), int(last or first) + 1)
        for m in range(1, args.months + 1)
    ]
    os.makedirs(args.scratch, exist_ok=True)
    catalog = SqlCatalog(
        "taxi",
        uri=f"sqlite:///{args.scratch}/catalog.db",
        warehouse=args.warehouse.rstrip("/"),
        **s3_properties(),
    )
    catalog.create_namespace_if_not_exists("taxi")
    if catalog.table_exists("taxi.trips"):
        catalog.drop_table("taxi.trips")
    table = catalog.create_table(
        "taxi.trips",
        schema=SCHEMA,
        partition_spec=PartitionSpec(
            PartitionField(
                source_id=2, field_id=1000, transform=MonthTransform(), name="pickup_month"
            )
        ),
        properties={"format-version": "2"},
    )

    # Downloads run ahead of the appends, which go one month at a time.
    total, dropped_total, started = 0, 0, time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(8) as pool:
        files = [pool.submit(download, args.scratch, y, m) for y, m in months]
        for (year, month), fut in zip(months, files):
            path = fut.result()
            t0 = time.monotonic()
            batch, dropped = in_month(normalise(pq.read_table(path)), year, month)
            table.append(batch)
            os.remove(path)
            total += batch.num_rows
            dropped_total += dropped
            print(
                f"{year}-{month:02d}  {batch.num_rows:>11,} rows"
                f"  (-{dropped:>5} out of month)  {time.monotonic() - t0:5.1f}s",
                flush=True,
            )

    files = len(table.inspect.files())
    print(f"\n{total:,} rows in {len(months)} appends, {files} data files,"
          f" {time.monotonic() - started:.0f}s")
    print(f"{dropped_total:,} rows dropped as out of month")
    print(f"metadata: {table.metadata_location}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
