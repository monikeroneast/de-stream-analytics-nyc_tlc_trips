"""
Kafka Consumer — NYC Yellow Taxi Data

Consumes messages from a Kafka topic and writes them as newline-delimited
JSON (NDJSON) files to Google Cloud Storage, acting as the pipeline's
data lake ingestion layer.

The consumer batches messages and writes them in chunks to GCS to balance
between latency and efficiency.

Usage:
    python consumer.py [--topic TOPIC] [--bootstrap SERVERS] [--bucket BUCKET]
"""

import argparse #built-in library used to parse command-line options and arguments
import json #imports the JSON encoder and decoder module to handle structured data format
import logging #imports Python's standard logging system to track application events and errors
import signal #imports the system signal module to intercept termination signals like Ctrl+C
import sys #imports system-specific functions, allowing direct interaction with the Python interpreter
import os #imports OS interaction tools to fetch system environment variables
from datetime import datetime #imports datetime objects to generate timestamps for filenames and pathways
from pathlib import Path #imports modern object-oriented filesystem path manipulation utilities for directories and paths
from io import BytesIO #imports an in-memory stream for byte data (not directly used here)

from confluent_kafka import Consumer, KafkaError, KafkaException #imports confluent-kafka tools to manage connections, fetch data, and catch messaging faults
from google.cloud import storage #imports the official Google Cloud client library to interact with GCS

# ──────────────────────────────────────────────
# CONFIGURATION SETTINGS
# ──────────────────────────────────────────────

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092" #local address and port for the Kafka broker
KAFKA_TOPIC = "yellow_taxi_trips" #stream name to pull data from
KAFKA_GROUP_ID = "nyc-tlc-taxi-trips-consumer-group" #consumer identity group to coordinate reading offsets
GCS_BUCKET = os.getenv("GCS_BUCKET_NAME") #targeted GCS bucket name from system variables
GCS_PREFIX = "raw/yellow_taxi" #the root folder structure inside the storage bucket
BATCH_SIZE = 5000  # Messages per file; constraint for grouping messages before file uploads
LOCAL_BACKUP_DIR = Path(__file__).resolve().parent.parent / "data" / "consumed"
#create an absolute fallback folder path for storage if the cloud fails
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
#the global logger to output timestamps, severity labels, and details
logger = logging.getLogger(__name__) #specific logger instance using the current module's namespace

# Graceful shutdown
shutdown_flag = False #boolean tracker to dictate whether the script should terminate

# ──────────────────────────────────────────────
# SHUTDOWN SIGNAL INERTECEPTION
# ──────────────────────────────────────────────

def signal_handler(sig, frame): #task to execute when the operating system interrupts the script
    global shutdown_flag #global flag variable to modify its state inside the scope
    logger.info("Shutdown signal received. Finishing current batch...") # informational status update declaring that the script is ending
    shutdown_flag = True #global switch to True to break the infinite execution loop


signal.signal(signal.SIGINT, signal_handler) #binds the the interruption event (Ctrl+C) to your custom signal handler
signal.signal(signal.SIGTERM, signal_handler) #binds the system kill command (termination request) to your handler

# ──────────────────────────────────────────────
# CREATING KAFKA CONSUMER
# ──────────────────────────────────────────────

def create_consumer(bootstrap_servers: str, group_id: str, topic: str) -> Consumer:
    """Create and subscribe a Kafka Consumer.
        #method requiring configuration
    """
    config = {
        "bootstrap.servers": bootstrap_servers, #location of your active streaming platform clusters
        "group.id": group_id, #identifier for tracking among shared application instances
        "auto.offset.reset": "earliest", #orders to start reading historic data from the beginning if no offset exists
        "enable.auto.commit": True, #automated progress logging back to the Kafka broker
        "auto.commit.interval.ms": 5000, #to run once every 5 seconds
        "max.poll.interval.ms": 300000, #5-minute health check window before eviction from the consumer group
        "session.timeout.ms": 45000, #45-second heartbeat deadline to detect dead application workers
    }
    consumer = Consumer(config) #active streaming kafka counsumer client object 
    consumer.subscribe([topic]) #the streaming connection to bind exclusively to the specified topic list
    logger.info(f" Consumer subscribed to topic '{topic}'") #a diagnostic tracking statement confirming subscription success
    return consumer #return the functional ingestion consumer object back to the request source

# ──────────────────────────────────────────────
# GCS AND LOCAL FILE DRIVERS
# ──────────────────────────────────────────────

def upload_to_gcs(bucket_name: str, blob_path: str, data: str) -> bool: #delivery process returning a success or failure status flag
    """Upload a string of NDJSON data to Google Cloud Storage."""
    try: #error-catching perimeter around unstable network tasks
        client = storage.Client() #an active network operations controller with your Google Cloud platform
        bucket = client.bucket(bucket_name) #the cloud storage folder bucket specified in configurations
        blob = bucket.blob(blob_path) #creates a specific file landing path inside the target cloud bucket
        blob.upload_from_string(data, content_type="application/json") #streams the literal text blob directly into cloud storage over HTTPS
        logger.info(f" Uploaded to gs://{bucket_name}/{blob_path}") #completion confirmation message
        return True #the upload succeeded
    except Exception as e: #catches any active network drops, authentication errors, or upload failures
        logger.error(f" Failed to upload to GCS: {e}") #publishes an error alert 
        return False #return a failure code


def save_local_backup(data: str, file_name: str): #fallback storage function to prevent data loss
    """Save data locally as a fallback if GCS upload fails."""
    LOCAL_BACKUP_DIR.mkdir(parents=True, exist_ok=True) #local backup folders on your disk
    file_path = LOCAL_BACKUP_DIR / file_name #computes the exact file path on the host
    file_path.write_text(data) #writes the payload into local drive storage
    logger.info(f" Local backup saved to {file_path}") #local disk tracking

# ──────────────────────────────────────────────
# BATCH FLUSHING
# ──────────────────────────────────────────────


def flush_batch(messages: list, bucket_name: str, batch_num: int) -> int:
    if not messages:
        return 0

    # 1. Parse the very first message in the batch to find its true event date
    try:
        sample_record = json.loads(messages[0])
        # Extract '2026-01-24T09:36:19' -> split to get '2026-01-24'
        pickup_date_str = sample_record.get("tpep_pickup_datetime", "").split("T")[0]
        # Parse into a datetime object safely
        event_date = datetime.strptime(pickup_date_str, "%Y-%m-%d")
    except Exception:
        # Safe fallback to system time if a record is corrupted
        event_date = datetime.utcnow()

    # 2. Build a production Hive-style partitioned path
    now = datetime.utcnow()
    file_name = f"trips_{now.strftime('%Y%m%d_%H%M%S')}_{batch_num:06d}.json"
    
    # Generates raw/yellow_taxi/year=2026/month=01/day=24/trips_...json
    blob_path = f"{GCS_PREFIX}/year={event_date.strftime('%Y')}/month={event_date.strftime('%m')}/day={event_date.strftime('%d')}/{file_name}"

    ndjson_content = "\n".join(messages) + "\n"
    success = upload_to_gcs(bucket_name, blob_path, ndjson_content)

    if not success:
        save_local_backup(ndjson_content, file_name)

    return len(messages)

# ──────────────────────────────────────────────
# MESSAGE LOOP
# ──────────────────────────────────────────────
#the central loop that processes message streams from Kafka
def consume_messages(
    consumer: Consumer,
    bucket_name: str,
    batch_size: int = BATCH_SIZE,
):
    """
    Main consume loop: read messages from Kafka, batch them, and flush to GCS.
    """
    global shutdown_flag #Gives the system tracking engine authority to read external closure alerts

    batch = [] #open array to accumulate inbound row entries
    batch_num = 0 #an incremental file counter starting from zero
    total_consumed = 0 #a cumulative record of all processed events 

    logger.info(f" Starting consumer loop (batch_size={batch_size})...") #data ingestion is active

    try:
        while not shutdown_flag: #loops continuously until a shutdown signal changes the variable state
            msg = consumer.poll(timeout=1.0) #polls Kafka for a single message, waiting up to 1.0 second

            if msg is None: #an empty poll response
                # No message available — if we have a partial batch, flush it
                if batch: #checks if there is any unwritten data left in the memory
                    count = flush_batch(batch, bucket_name, batch_num) #Flushes the partial batch to GCS 
                    total_consumed += count #updates the total lifetime event log counter
                    batch_num += 1 #increments the file sequence number for the next batch
                    batch = [] #empties the memory container to prepare for new data
                    logger.info(f" Flushed partial batch. Total consumed: {total_consumed:,}") #writes out progress metrics
                continue #skips the rest of the loop and starts the next poll cycle

            if msg.error(): #whether the fetched data contains connection or topic exceptions
                if msg.error().code() == KafkaError._PARTITION_EOF: #if the consumer reached the end of the partition
                    logger.info(
                        f" Reached end of partition {msg.partition()} at offset {msg.offset()}"
                    ) #logs that the stream has caught up to the latest message
                    # Flush remaining messages
                    if batch: #verifies if there are residual messages waiting in the buffer
                        count = flush_batch(batch, bucket_name, batch_num) #flushes any remaining data to GCS
                        total_consumed += count #updates the event tracking counter
                        batch_num += 1 #increments the filename index integer
                        batch = [] #wipes the array cache clean
                    continue #jumps out of error filtering to begin reading new streams
                else:
                    raise KafkaException(msg.error()) #a fatal exception

            # Process valid message
            value = msg.value().decode("utf-8") #decodes the raw binary payload from Kafka into a UTF-8 text string
            batch.append(value) #inserts the structured data row into the active memory array

            # Flush when batch is full
            if len(batch) >= batch_size: #checks if the accumulated batch has hit the size limit
                count = flush_batch(batch, bucket_name, batch_num) #saves the complete batch data to GCS or local backup
                total_consumed += count #updates the global tracking log with the new counts
                batch_num += 1 #advances the incremental filename sequence number
                batch = [] #refreshes the memory storage container to empty
                logger.info(f"Total consumed: {total_consumed:,}") #logs the updated total message count to the console

    except KeyboardInterrupt: #catches manually triggered local termination requests
        logger.info("Consumer interrupted by user") #logs a user-initiated exit message
    finally: #ensures cleanup actions run even if the script crashes or closes
        # Flush any remaining messages
        if batch: #checks one last time for leftover messages in memory during shutdown
            count = flush_batch(batch, bucket_name, batch_num) #commits the final records to safe storage
            total_consumed += count #saves final metrics into the total counter variable
            logger.info(f" Flushed final batch of {count} messages") #final update

        consumer.close() #loses the Kafka consumer connection and releases network resources
        logger.info(f" Consumer shut down. Total messages consumed: {total_consumed:,}")
        #consumer termination statement

# ──────────────────────────────────────────────
# THE ENTRY POINT MAIN FUNCTION
# ──────────────────────────────────────────────

def main(): #loading and execution steps into a main function
    parser = argparse.ArgumentParser(description="Kafka Consumer for NYC Taxi Data → GCS") #interface instance to process runtime CLI settings
    parser.add_argument("--topic", type=str, default=KAFKA_TOPIC, help="Kafka topic name") #Registers the optional command-line flag --topic to override the target Kafka stream 
    parser.add_argument(
        "--bootstrap",
        type=str,
        default=KAFKA_BOOTSTRAP_SERVERS,
        help="Kafka bootstrap servers",
    ) #Registers --bootstrap to change broker network addresses dynamically
    parser.add_argument(
        "--group-id", type=str, default=KAFKA_GROUP_ID, help="Consumer group ID"
    ) #Registers --group-id to run parallel testing with different consumer identity groups
    parser.add_argument(
        "--bucket",
        type=str,
        default=GCS_BUCKET,
        help="GCS bucket name for data lake",
    ) #Registers --bucket to redirect data outputs to a different cloud storage bucket
    parser.add_argument(
        "--batch-size", type=int, default=BATCH_SIZE, help="Messages per GCS file"
    ) #Registers --batch-size to adjust performance thresholds dynamically
    args = parser.parse_args() #Parses the command-line arguments provided during script execution

    consumer = create_consumer(args.bootstrap, args.group_id, args.topic) #instantiates the Kafka driver using either command-line inputs or default values
    consume_messages(consumer, args.bucket, args.batch_size) #the primary consumer processing loop


if __name__ == "__main__": #file is being run directly by the user rather than imported as a library module
    main() #entry point to run the entire script