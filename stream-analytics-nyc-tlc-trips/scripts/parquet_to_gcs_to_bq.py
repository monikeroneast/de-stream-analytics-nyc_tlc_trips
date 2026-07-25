"""
Upload the local downloaded parquet files to GCS and then BigQuery via python script.
1. From local to GCS.
2. From GCS to BigQuery.
Usage python scripts/parquet_to_gcs_bq.py
"""
#libraries
#____________________________
import os
from pathlib import Path
import re
from google.cloud import storage, bigquery
import logging

#Configuration
#____________________________
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "stream-analytics-nyc-tlc-trips") #get environment variable for project id
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", f"{GCP_PROJECT_ID}-taxi-data-lake") #get environment variable for bucket name
BQ_DATASET_NAME = os.environ.get("BQ_DATASET_NAME", "taxi_analytics") #get environment variable for dataset name
BQ_TABLE_NAME = "yellow_taxi_raw" #get environment variable for table name
DATA_DIR = Path(__file__).resolve().parent.parent/ "data" #get environment variable for data directory

#logging runtime print formats for messages
#____________________________
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

#Upload parquet files to GCS
#____________________________
def upload_parquet_to_gcs(local_file_path: Path, gcs_bucket_name: str, gcs_blob_name: str):
    """
    Upload a local parquet file to GCS bucket.
    Args:
        local_file_path (Path): Path to the local parquet file.
        gcs_bucket_name (str): Name of the GCS bucket.
        gcs_blob_name (str): Name of the blob(placeholder file) in GCS. 
    """
    try:
        # Initialize a GCS client
        storage_client = storage.Client(project=GCP_PROJECT_ID)
        bucket = storage_client.bucket(gcs_bucket_name)
        blob = bucket.blob(gcs_blob_name)

        # Upload the file
        logger.info(f"Uploading {local_file_path.name} → gs://{gcs_bucket_name}/{gcs_blob_name}")
        blob.upload_from_filename(local_file_path)
        #logger.info(f"File {local_file_path} uploaded to {gcs_blob_name} in bucket {gcs_bucket_name}.")
        logger.info(f"Upload complete ({local_file_path.stat().st_size / 1e6:.1f} MB)")
    except Exception as e:
        logger.error(f"Failed to upload {local_file_path} to GCS: {e}")

    

#Upload from GCS to BigQuery
#____________________________    
def load_parquet_from_gcs_to_bq(gcs_bucket_name: str, gcs_blob_name: str, 
                                bq_dataset_name: str, bq_table_name: str):
    """
    Load a parquet file from GCS to BigQuery.
    Args:
        gcs_bucket_name (str): Name of the GCS bucket.
        gcs_blob_name (str): Name of the blob(placeholder file) in GCS. 
        bq_dataset_name (str): Name of the BigQuery dataset.
        bq_table_name (str): Name of the BigQuery table.
    """
    try:
        # Initialize a BigQuery client
        bq_client = bigquery.Client(project=GCP_PROJECT_ID)
        dataset_ref = bq_client.dataset(bq_dataset_name)
        table_ref = dataset_ref.table(bq_table_name)

        # Configure the load job. 
        # First time load will overwrite (truncate and create) the table, 
        # subsequent loads will append to the table.
        # Optimization via partitioning and clustering can be done here if needed.
       
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, #daily partitioning
            field="tpep_pickup_datetime", #partitioning field
        ),
            clustering_fields=["PULocationID", "payment_type"],
        )

        # Load data from GCS to BigQuery
        uri = f"gs://{gcs_bucket_name}/{gcs_blob_name}"
        logger.info(f"Loading {uri} → {table_ref} (mode={'APPEND'})")
        logger.info("Partitioning: DAY on tpep_pickup_datetime")
        logger.info("Clustering: PULocationID, payment_type")

        load_job = bq_client.load_table_from_uri(uri, table_ref, job_config=job_config)
        load_job.result()  # Wait for the job to complete

        table = bq_client.get_table(table_ref)
        logger.info(f"Loaded into {table_ref} — total rows now: {table.num_rows:,}")
        logger.info(f"Table size: {table.num_bytes / 1e9:.2f} GB")

        #logger.info(f"Loaded data from {uri} to {bq_dataset_name}.{bq_table_name}.")
    except Exception as e:
        logger.error(f"Failed to load data from GCS to BigQuery: {e}")

    
    
#Main function to orchestrate the upload and load process
#____________________________
def main():
    # Find all parquet files in data/
    parquet_files = sorted(DATA_DIR.glob("*.parquet"))
    if not parquet_files:
        logger.error(f"No parquet files found in {DATA_DIR}")
        logger.info("Run: bash scripts/download_data.sh")
        return

    table_ref = f"{GCP_PROJECT_ID}.{BQ_DATASET_NAME}.{BQ_TABLE_NAME}"

    for i, pf in enumerate(parquet_files):
        # 1. Parse date elements from filename using regex
        # Example: 'yellow_tripdata_2026-01.parquet' -> ('2026', '01')
        match = re.search(r"(\d{4})-(\d{2})", pf.name)
        if not match:
            logger.error(f"Could not parse year/month from filename: {pf.name}, skipping.")
            continue
        
        year, month = match.group(1), match.group(2)
        # 2. THE PATH FIX: Construct a standard Hive Partitioning string
        blob_name = f"raw/yellow_taxi/year={year}/month={month}/{pf.name}"
        logger.info(f"\nProcessing file {i+1}/{len(parquet_files)}: {pf.name}")

        # Step 1: Upload to GCS (Data Lake)
        upload_parquet_to_gcs(pf, GCS_BUCKET_NAME, blob_name)

        # Step 2: Load from GCS to BigQuery (first file truncates, rest append)
        load_parquet_from_gcs_to_bq(GCS_BUCKET_NAME, blob_name, table_ref, BQ_TABLE_NAME)

    logger.info("Upload completed. Data is now in BigQuery.")


if __name__ == "__main__":
    main()