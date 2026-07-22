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
    project = var.project
    region  = var.region
    zone    = var.zone
}

# Provisioning Provider Resources BUCKET
# 3. Google Cloud Storage Bucket Resource
resource "google_storage_bucket" "data-lake-bucket" {
  name                        = "stream-analytics-nyc-tlc-trips-data-lake" 
  location                    = "ASIA-SOUTH2" # Best practice: uppercase for storage regions
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

# Provisioning Provider Resources BIGQUERY DATASET
# 4. BigQuery Dataset Resource
resource "google_bigquery_dataset" "dataset" {
  # FIX: Removed angle brackets. Dataset IDs only allow letters, numbers, and underscores.
  dataset_id = "nyc_tlc_trips" 
  project    = var.project
  location   = "asia-south2" # BigQuery dataset locations are lowercase
}