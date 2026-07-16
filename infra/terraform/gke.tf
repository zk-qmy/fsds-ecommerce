# Standard mode, zonal (not regional/Autopilot) -- a single always-on
# LocalExecutor Airflow pod (Spark runs local[n] in-pod, not on a separate
# Spark cluster) doesn't benefit from Autopilot's per-pod elastic billing
# since it never scales to zero; a flat per-vCPU/GB node pool is simpler to
# reason about and generally cheaper at this always-on, single-workload scale.
resource "google_container_cluster" "primary" {
  name     = "fsds-ecommerce-cluster"
  location = var.zone
  network  = data.google_compute_network.default.id

  # Node pool is managed as a separate google_container_node_pool resource
  # below, for control over machine type / size -- the cluster's own default
  # pool is removed immediately after creation.
  remove_default_node_pool = true
  initial_node_count       = 1

  workload_identity_config {
    workload_pool = "${var.project_id}.svc.id.goog"
  }

  deletion_protection = false
}

resource "google_container_node_pool" "primary" {
  name     = "fsds-primary-pool"
  cluster  = google_container_cluster.primary.name
  location = var.zone

  node_count = 1

  node_config {
    # 4 vCPU / 16GB -- Spark local[n] inside the Airflow pod needs real
    # headroom (n_cores = max(2, cpu_count - 2), see pipeline_base.py),
    # not a minimal e2-small.
    machine_type = "e2-standard-4"
    disk_size_gb = 50

    workload_metadata_config {
      mode = "GKE_METADATA"
    }

    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform",
    ]
  }
}
