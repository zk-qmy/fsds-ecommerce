output "cluster_name" {
  value = google_container_cluster.primary.name
}

output "cluster_zone" {
  value = var.zone
}

output "cloudsql_connection_name" {
  description = "project:region:instance -- the Cloud SQL Auth Proxy sidecar's connection string argument."
  value       = google_sql_database_instance.primary.connection_name
}

output "pg_password" {
  description = "Generated Cloud SQL 'fsds' user password -- feed this into the fsds_postgres Airflow Connection at deploy time, not the illustrative value in the DAG docstrings."
  value       = random_password.pg_password.result
  sensitive   = true
}

output "airflow_gsa_email" {
  description = "GCP service account email -- goes in infra/helm/airflow serviceaccount.yaml's iam.gke.io/gcp-service-account annotation."
  value       = google_service_account.airflow.email
}

output "bronze_bucket" {
  value = google_storage_bucket.bronze.name
}

output "silver_bucket" {
  value = google_storage_bucket.silver.name
}
