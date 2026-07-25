"""
Kafka Producer — NYC Yellow Taxi Data

Reads a Parquet file of NYC Yellow Taxi trip records and publishes
each row as a JSON message to a Kafka topic, simulating real-time
data ingestion.

Usage:
    python producer.py [--file DATA_FILE] [--topic TOPIC] [--bootstrap SERVERS]
"""

import argparse #parse command line arguments
import json #utility to convert Python dictionary objects to JSON strings
import time #utlity to control stream speed using time delays
import logging #standard loggin framework to print formatted messages to the console
from pathlib import Path #object-oriented filesystem path tool to manage file paths

import pandas as pd #pandas library to read parquet files and manipulate dataframes
from confluent_kafka import Producer #Apache Kafka client library wrapper 

#____________________________________
# CONFIGURATION SETTINGS
#____________________________________
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092" #localhost address for kafka server cluster
KAFKA_TOPIC = "yellow_taxi_trips" #kafka topic name for yellow taxi trip records
DATA_DIR = Path(__file__).resolve().parent.parent / "data" #the absolute path to a default folder named data located two levels up

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
) #Configures global tracking logs to output the time, severity level, and specific text
logger = logging.getLogger(__name__) #logging entity specific to this execution scope

#____________________________________
#DELIVERY CALLBACK FUNCTION
#____________________________________

def delivery_report(err, msg): #asynchronous feedback function executed by Kafka after trying to send a message
    """Callback invoked once per produced message to indicate delivery result."""
    if err is not None: #network or server delivery error occurred during transmission
        logger.error(f" Message delivery failed: {err}") #details of the failed network message transmission
    else: #successful outcome if no transmission error
        logger.debug(
            f" Message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}"
        ) #Kafka topic name, partition index, and sequence number for verification

#____________________________________
#CREATING KAFKA PRODUCER
#____________________________________

def create_producer(bootstrap_servers: str) -> Producer:
    """Create and return a Kafka Producer instance."""
    config = {
        "bootstrap.servers": bootstrap_servers, #destination network locations of the Kafka brokers
        "client.id": "taxi-producer", #application's logical identity inside Kafka server logs
        "acks": "all", #all cluster replicas to acknowledge receipt
        "retries": 3, #the producer client to resend failed messages up to three times automatically
        "linger.ms": 10, #delay network transmission by 10 milliseconds to bundle small single messages together
        "batch.size": 65536, #maximum data transmission batches to 64 Kb
        "compression.type": "snappy", #compresses data using Google's Snappy algorithm to save network bandwidth
    }
    return Producer(config) #instantiates and passes back the ready-to-use live client pipeline object


#____________________________________
#LOADING PARQUET DATA
#____________________________________

def read_parquet_data(file_path: str) -> pd.DataFrame: #function to ingest the compressed storage file format
    """Read Parquet file and return a DataFrame."""
    logger.info(f" Reading data from {file_path}") #outputs a timestamped status text indicating which target data file is being opened
    df = pd.read_parquet(file_path) #loads the binary Parquet file directly into an in-memory tabular data frame structure
    logger.info(f" Loaded {len(df):,} records") #prints the total count of loaded data records
    return df

#____________________________________
#PROCESSING AND PUBLISHING THE MESSAGE
#____________________________________

def produce_messages(producer: Producer, chunk_records: list, topic: str):
    """Streams a pre-processed list of dictionary records to Kafka instantly."""
    sent = 0
    for record in chunk_records:
        try:
            # Clean null values safely
            clean_record = {k: (None if pd.isna(v) else v) for k, v in record.items()}
            
            # Format datetime objects explicitly to string layouts
            for k, v in clean_record.items():
                if hasattr(v, 'isoformat'):
                    clean_record[k] = v.isoformat()

            key = str(clean_record.get("PULocationID", "unknown"))
            value = json.dumps(clean_record)

            producer.produce(
                topic=topic,
                key=key,
                value=value,
                callback=delivery_report,
            )
            sent += 1
        except BufferError:
            producer.flush()
            producer.produce(topic=topic, key=key, value=value, callback=delivery_report)
            sent += 1
        except Exception:
            pass
            
    # Force network transmission at the end of every chunk
    producer.flush()
    return sent

#____________________________________
#THE ENTRY POINT MAIN FUNCTION
#____________________________________

def main(): #sets up runtime execution parameters
    parser = argparse.ArgumentParser(description="Kafka Producer for NYC Taxi Data") #command terminal configuration argument processor utility instance
    #Registers an optional file pathway flag defaulting to an exact January 2026 parquet file
    parser.add_argument(
        "--file",
        type=str,
        default=str(DATA_DIR / "yellow_tripdata_2026-01.parquet"),
        help="Path to the Parquet data file",
    )
    #Registers a flag option to overwrite the targeted destination Kafka topic stream channel
    parser.add_argument("--topic", type=str, default=KAFKA_TOPIC, help="Kafka topic name")
    #Registers a flag allowing easy reconfiguration of target network server IP addresses
    parser.add_argument(
        "--bootstrap",
        type=str,
        default=KAFKA_BOOTSTRAP_SERVERS,
        help="Kafka bootstrap servers",
    )
    #Registers a flag to tune how many items accumulate before network flushes happen
    parser.add_argument(
        "--batch-size", type=int, default=1000, help="Messages per batch before flush"
    )
    #Registers an argument flag to control execution pacing delays
    parser.add_argument(
        "--delay", type=float, default=0.01, help="Delay in seconds between batches"
    )
    args = parser.parse_args() #Evaluates and binds terminal input choices directly into a usable configuration variable object

    # Verify data file exists
    #validates whether the targeted parquet input data actually exists on disk
    if not Path(args.file).exists():
        logger.error(f" Data file not found: {args.file}") #logs a failure alert explaining that the requested input file cannot be opened
        logger.info("Run 'bash scripts/download_data.sh' first to download the dataset.") #prompt the user to use a downloading helper script to retrieve the needed files
        return #stops the execution completely to avoid runtime code crashes

    producer = create_producer(args.bootstrap) #spins up the active streaming connection to the Kafka cluster brokers
    df = read_parquet_data(args.file) #ingests the raw table data file directly into system processing memory
    produce_messages(producer, df, args.topic, args.batch_size, args.delay) #executes the main streaming pipeline using the processed configuration settings


if __name__ == "__main__": #the main runner script fires only if explicitly executed by the Python runtime directly
    #main() #Triggers the top-level orchestrator block to kick off execution

    parser = argparse.ArgumentParser(description="Kafka Production Engine")
    parser.add_argument("--file", type=str, default="/workspaces/de-stream-analytics-nyc_tlc_trips/stream-analytics-nyc-tlc-trips/data/yellow_tripdata_2026-01.parquet")
    parser.add_argument("--topic", type=str, default=KAFKA_TOPIC)
    parser.add_argument("--bootstrap", type=str, default=KAFKA_BOOTSTRAP_SERVERS)
    args = parser.parse_args()

    try:
        producer_instance = create_producer(args.bootstrap)
        logger.info(f"Opening Parquet file in chunked streaming mode: {args.file}")
        
        # Read the parquet file in controlled batches using PyArrow
        parquet_file = pd.ExcelFile(args.file) if args.file.endswith('.xlsx') else None # Fallback block
        
        import pyarrow.parquet as pq
        pf = pq.ParquetFile(args.file)
        
        total_records = pf.metadata.num_rows
        running_total = 0
        
        logger.info(f"Total file size: {total_records:,} rows. Starting batch processing...")

        # Process the file in groups of 10,000 rows
        for batch in pf.iter_batches(batch_size=10000):
            df_chunk = batch.to_pandas()
            chunk_records = df_chunk.to_dict(orient="records")
            
            # Stream the current chunk
            records_sent = produce_messages(producer_instance, chunk_records, args.topic)
            running_total += records_sent
            
            logger.info(f"Progress: {running_total:,}/{total_records:,} records successfully pushed to Kafka ({100*running_total/total_records:.1f}%)")
            time.sleep(0.1) # Brief pause to prevent network saturation

        logger.info("Data pipeline streaming process completed successfully!")

    except Exception as fatal_err:
        logger.critical(f"Process crashed unexpectedly: {fatal_err}")