variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region for all resources"
  type        = string
  default     = "us-central1"
}

variable "bucket_name" {
  description = "GCS bucket for raw FEC data"
  type        = string
}

variable "credentials_file" {
  description = "Path to the GCP service account JSON key file"
  type        = string
  default     = "../gcp.json"
}
