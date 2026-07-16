# Workload Identity: binds the Kubernetes ServiceAccount (created by
# infra/helm/airflow's serviceaccount.yaml) to this GCP service account, so
# the Airflow pod authenticates as this GSA with no static keys anywhere --
# picked up automatically by both the Spark GCS connector (COMPUTE_ENGINE
# auth type, pipeline_base.py) and delta-rs (validation_runner.py) via the
# GKE metadata-server proxy.
resource "google_service_account" "airflow" {
  account_id   = "fsds-airflow-gsa"
  display_name = "fsds Airflow/Spark pipeline SA"
}

# Bucket-scoped, not project-level roles/storage.objectAdmin -- narrower
# blast radius, one binding per bucket.
resource "google_storage_bucket_iam_member" "airflow_bronze" {
  bucket = google_storage_bucket.bronze.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.airflow.email}"
}

resource "google_storage_bucket_iam_member" "airflow_silver" {
  bucket = google_storage_bucket.silver.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.airflow.email}"
}

# Cloud SQL has no instance-level IAM binding -- roles/cloudsql.client is
# necessarily project-scoped. Needed by the Cloud SQL Auth Proxy sidecar
# (infra/helm/airflow) to open a connection via the Cloud SQL Admin API.
resource "google_project_iam_member" "airflow_cloudsql_client" {
  project = var.project_id
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:${google_service_account.airflow.email}"
}

# The actual KSA<->GSA link -- lets the Kubernetes ServiceAccount
# "data-pipelines/fsds-airflow-ksa" impersonate this GSA. The KSA itself is
# created by infra/helm/airflow's serviceaccount.yaml (Helm doesn't manage
# GCP IAM), and must carry the matching
# `iam.gke.io/gcp-service-account: <this GSA's email>` annotation for the
# link to actually take effect.
resource "google_service_account_iam_member" "airflow_workload_identity" {
  service_account_id = google_service_account.airflow.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[${var.gke_namespace}/${var.airflow_ksa_name}]"

  # Confirmed live: without this, Terraform has no dependency-graph edge to
  # the cluster (this resource's fields never reference
  # google_container_cluster.primary), so it can race the cluster's
  # workload_identity_config -- the Identity Pool
  # (<project>.svc.id.goog) takes a few seconds to propagate after the
  # cluster reports created, and binding against it too early fails with
  # "Identity Pool does not exist".
  depends_on = [google_container_cluster.primary]
}
