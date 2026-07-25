#!/bin/bash
#-e: exit on any commanderror; 
#-u: exit on undefined variable; 
#-o: exit on command error in a pipeline
set -euo pipefail

#Download the NYC TLC Trip Data from the TLC website and upload it to the GCS bucket
#Default to the 2026 Taxi Trip Data, Q1 (Jan, Feb, Mar)

YEAR="${1:-2026}"
MONTHS="${2:-01 02 03}"

#Base URL for the NYC TLC Trip Data
#https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-01.parquet
BASE_URL="https://d37ci6vzurychx.cloudfront.net/trip-data" 
#Data directory to store the downloaded files. 
#Create a folder data in the absolute path from the script directory, within the project directory
DATA_DIR="$(cd "$(dirname "$0")/.." && pwd)/data"
#make the data directory if it doesn't exist
mkdir -p "${DATA_DIR}"
#print details about the download on the terminal
echo "Downloading the NYC TLC Trip Data for Year: ${YEAR}, from ${BASE_URL}"
echo "Storing it in ${DATA_DIR}"
echo ""

#Loop through the months and download the data files
for MONTH in ${MONTHS}; do
    FILE_NAME="yellow_tripdata_${YEAR}-${MONTH}.parquet"
    FILE_URL="${BASE_URL}/${FILE_NAME}"
    DESTINATION_FILE="${DATA_DIR}/${FILE_NAME}"

    #if file already exists, skip the download
    if [ -f "${DESTINATION_FILE}" ]; then
        echo "File ${DESTINATION_FILE} already exists. Skipping download."
    else
        echo "Downloading ${FILE_URL} to ${DESTINATION_FILE}"
        curl -o "${DESTINATION_FILE}" "${FILE_URL}"
    fi
done
#print the list of downloaded files in the data directory on the terminal
echo ""
echo "Download complete. Uploading the files to the GCS bucket."
#list, -l: long format, -h: human-readable sizes, File Descriptor 2> Backhole /dev/null: redirect error output to /dev/null
ls -lh "${DATA_DIR}"/*.parquet 2>/dev/null || echo "No such files."