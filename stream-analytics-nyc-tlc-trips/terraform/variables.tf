variable "project_id" {
  type        = string
  description = "The ID of the GCP project where resources will be deployed"
  default     = "stream-analytics-nyc-tlc-trips"
}

variable "region" {
  type        = string
  description = "The target regional data center location"
  default     = "asia-south2" # Delhi, India Data Center
}

variable "zone" {
  type        = string
  description = "The target availability zone within the region"
  default     = "asia-south2-a"
}

variable "gcs_bucket_name" {
  description = "Name suffix for the GCS data lake bucket"
  type        = string
  default     = "taxi-data-lake"
}

variable "bq_dataset" {
  description = "BigQuery dataset name"
  type        = string
  default     = "taxi_analytics"
}




