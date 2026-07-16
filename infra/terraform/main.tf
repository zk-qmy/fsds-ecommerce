terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Default VPC -- a custom network only pays off for private-IP Cloud SQL or
# multi-environment isolation, neither needed here since the Cloud SQL Auth
# Proxy sidecar (infra/helm/airflow) talks to Cloud SQL over its public
# endpoint via ephemeral mTLS certs, not a raw socket -- no VPC peering
# required.
data "google_compute_network" "default" {
  name = "default"
}
