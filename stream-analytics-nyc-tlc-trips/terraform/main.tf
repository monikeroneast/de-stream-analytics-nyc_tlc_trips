# 1. ADC/ Provider Configuration
# ADC (Application Default Configuration)/ Provider Configuration 
terraform {
    required_providers {
        google = {
            source  = "hashicorp/google"
            version = "4.51.0"
        }
    }
}

# 2. Primary Provider Block
# Connect to gcp using ADC (identity verification) & This implicitly uses the JSON key path declared in your .env file
provider "google" {
    project = var.project_id
    region  = var.region
    zone    = var.zone
}
#____________________________
#DATA LAKEHOUSE
#____________________________
# Provisioning Provider Resources BUCKET
# 3. Google Cloud Storage Bucket Resource
resource "google_storage_bucket" "data_lake" {
  name                        = "${var.project_id}-taxi-data-lake" 
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  force_destroy               = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30 # days
    }
  }
}

#____________________________
#DATA WAREHOUSE
#____________________________
# Provisioning Provider Resources BIGQUERY DATASET
# 4. BigQuery Dataset Resource
resource "google_bigquery_dataset" "taxi_dataset" {
  dataset_id = var.bq_dataset
  project    = var.project_id
  location   = var.region
  delete_contents_on_destroy = true

  labels = {
      environment = "dev"
      project     = "taxi-analytics"
    }
}

# Provisioning Provider Resources BIGQUERY DATASET TABLE
# 5. BigQuery Dataset Table Resource
resource "google_bigquery_table" "yellow_taxi_raw" {
  dataset_id          = google_bigquery_dataset.taxi_dataset.dataset_id
  table_id            = "yellow_taxi_raw"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "tpep_pickup_datetime"
  }

  clustering = ["PULocationID", "payment_type"]

  schema = file("${path.module}/schemas/yellow_taxi.json")

  labels = {
    environment = "dev"
  }
}