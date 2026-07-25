"""
GCS to BigQuery Loader

Reads NDJSON files from the GCS data lake and loads them into
BigQuery with proper partitioning and clustering.

Usage:
    python gcs_to_bq.py [--bucket BUCKET] [--dataset DATASET] [--table TABLE]
"""

import argparse
import logging
import os
from dotenv import load_dotenv

from google.cloud import bigquery, storage

load_dotenv()
# ──────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────
GCP_PROJECT = os.getenv("GCP_PROJECT_ID")
GCS_BUCKET = os.getenv("GCS_BUCKET_NAME")
GCS_PREFIX = "raw/yellow_taxi"
BQ_DATASET = "taxi_analytics"
BQ_TABLE = "yellow_taxi_raw"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def load_gcs_to_bigquery(
    project_id: str,
    bucket_name: str,
    prefix: str,
    dataset_id: str,
    table_id: str,
):
    """
    Load NDJSON files from GCS into BigQuery using a load job.

    The target table is partitioned by pickup datetime and clustered
    by pickup location and payment type for query performance.
    """
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    # Configure the load job
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=False,
        schema=[
            bigquery.SchemaField("VendorID", "INTEGER"),
            bigquery.SchemaField("tpep_pickup_datetime", "TIMESTAMP"),
            bigquery.SchemaField("tpep_dropoff_datetime", "TIMESTAMP"),
            bigquery.SchemaField("passenger_count", "INTEGER"),
            bigquery.SchemaField("trip_distance", "FLOAT"),
            bigquery.SchemaField("RatecodeID", "FLOAT"),
            bigquery.SchemaField("store_and_fwd_flag", "STRING"),
            bigquery.SchemaField("PULocationID", "INTEGER"),
            bigquery.SchemaField("DOLocationID", "INTEGER"),
            bigquery.SchemaField("payment_type", "INTEGER"),
            bigquery.SchemaField("fare_amount", "FLOAT"),
            bigquery.SchemaField("extra", "FLOAT"),
            bigquery.SchemaField("mta_tax", "FLOAT"),
            bigquery.SchemaField("tip_amount", "FLOAT"),
            bigquery.SchemaField("tolls_amount", "FLOAT"),
            bigquery.SchemaField("improvement_surcharge", "FLOAT"),
            bigquery.SchemaField("total_amount", "FLOAT"),
            bigquery.SchemaField("congestion_surcharge", "FLOAT"),
            bigquery.SchemaField("cbd_congestion_fee", "FLOAT"),
            bigquery.SchemaField("Airport_fee", "FLOAT"),
        ],
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="tpep_pickup_datetime",
        ),
        clustering_fields=["PULocationID", "payment_type"],
    )

    # GCS URI pattern — load all JSON files under the prefix
    uri = f"gs://{bucket_name}/{prefix}/*.json"

    logger.info(f" Loading data from {uri} → {table_ref}")
    logger.info(" Partitioning: DAY on tpep_pickup_datetime")
    logger.info(" Clustering: PULocationID, payment_type")

    load_job = client.load_table_from_uri(
        uri,
        table_ref,
        job_config=job_config,
    )

    # Wait for the job to complete
    logger.info("Waiting for load job to complete...")
    load_job.result()

    # Report results
    destination_table = client.get_table(table_ref)
    logger.info(f" Loaded {destination_table.num_rows:,} rows to {table_ref}")
    logger.info(f" Table size: {destination_table.num_bytes / 1e9:.2f} GB")


def main():
    parser = argparse.ArgumentParser(description="Load taxi data from GCS to BigQuery")
    parser.add_argument("--project", type=str, default=GCP_PROJECT, help="GCP project ID")
    parser.add_argument("--bucket", type=str, default=GCS_BUCKET, help="GCS bucket name")
    parser.add_argument("--prefix", type=str, default=GCS_PREFIX, help="GCS prefix path")
    parser.add_argument("--dataset", type=str, default=BQ_DATASET, help="BigQuery dataset")
    parser.add_argument("--table", type=str, default=BQ_TABLE, help="BigQuery table")
    args = parser.parse_args()

    load_gcs_to_bigquery(
        project_id=args.project,
        bucket_name=args.bucket,
        prefix=args.prefix,
        dataset_id=args.dataset,
        table_id=args.table,
    )


if __name__ == "__main__":
    main()