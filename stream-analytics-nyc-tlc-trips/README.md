## 🚀 How to Run

### 1. Provision Infrastructure
Authenticate with Google Cloud and build your GCP resources using Terraform:
```bash
cd terraform
terraform init
terraform apply
```

### 2. Start Streaming Pipeline
Spin up the Kafka environment and run your streaming scripts to move data to GCS:
```bash
# Start Kafka & Zookeeper
cd ../kafka
docker-compose up -d

# Run Python streaming scripts
python producer.py
python consumer.py
```

### 3. Load & Transform Data
Batch-load your raw streaming data from GCS into BigQuery, then build your models:
```bash
# Load data into BigQuery
python ../scripts/gcs_to_bq.py

# Run transformations
cd ../dbt/taxi_analytics
dbt deps
dbt run
```

### 4. Launch Dashboard
Fire up your local Streamlit instance to view the final analytical metrics:
```bash
cd ../../dashboard
streamlit run app.py
```
