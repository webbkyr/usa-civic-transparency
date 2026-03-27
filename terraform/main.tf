terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project     = var.project_id
  region      = var.region
  credentials = file(var.credentials_file)
}

# GCS
resource "google_storage_bucket" "raw" {
  name          = var.bucket_name
  location      = var.region
  force_destroy = false

  versioning {
    enabled = true
  }

  uniform_bucket_level_access = true
}

# BigQuery Datasets
resource "google_bigquery_dataset" "raw" {
  dataset_id  = "raw"
  location    = var.region
  description = "Raw data ingested from GCS — no transformations."
}

resource "google_bigquery_dataset" "staging" {
  dataset_id  = "staging"
  location    = var.region
  description = "Cleaned and typed data, one table per source file."
}

resource "google_bigquery_dataset" "reports" {
  dataset_id  = "reports"
  location    = var.region
  description = "Mart tables powering the civic transparency dashboard."
}
