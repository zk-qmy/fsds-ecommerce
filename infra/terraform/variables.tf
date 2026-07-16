variable "project_id" {
  description = "GCP project ID."
  type        = string
  default     = "fsds-ecommerce"
}

variable "region" {
  description = "GCP region for the cluster, Cloud SQL, and GCS buckets."
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "GCP zone for the (zonal, not regional) GKE cluster and its node pool."
  type        = string
  default     = "us-central1-a"
}

variable "gke_namespace" {
  description = "Kubernetes namespace the Airflow workload runs in -- referenced by the Workload Identity binding's member string."
  type        = string
  default     = "data-pipelines"
}

variable "airflow_ksa_name" {
  description = "Kubernetes ServiceAccount name Airflow's pod uses -- must match infra/helm/airflow's serviceaccount.yaml."
  type        = string
  default     = "fsds-airflow-ksa"
}
