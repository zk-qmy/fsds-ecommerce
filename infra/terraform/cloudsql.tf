# db-g1-small (1.7GB RAM), not db-f1-micro (0.6GB) -- confirmed both are
# still current, non-deprecated shared-core Postgres tiers, but neither is
# covered by Cloud SQL's SLA (dev/test only, explicitly documented as such --
# accepted tradeoff for coursework cost). db-f1-micro's 0.6GB is genuine
# OOM risk for Spark JDBC writes (build_gold.py) + GX reads (validation_runner.py).
resource "google_sql_database_instance" "primary" {
  name             = "fsds-pg"
  database_version = "POSTGRES_15"
  region           = var.region

  settings {
    tier = "db-g1-small"
    ip_configuration {
      # Public IP stays enabled but is never in any authorized-networks
      # allowlist -- the Cloud SQL Auth Proxy sidecar (infra/helm/airflow)
      # reaches the instance through the Cloud SQL Admin API via ephemeral
      # mTLS certs, not a direct socket to this IP. No Private Service
      # Access / VPC peering needed.
      ipv4_enabled = true
    }
  }

  deletion_protection = false
}

resource "google_sql_database" "fsds" {
  name     = "fsds"
  instance = google_sql_database_instance.primary.name
}

# Generated rather than hardcoded -- the DAG docstrings' `airflow connections
# add fsds_postgres --conn-password fsds` is illustrative of the command
# shape only; the real deploy-time value comes from `terraform output
# -raw pg_password`, never this literal string.
resource "random_password" "pg_password" {
  length  = 24
  special = false
}

resource "google_sql_user" "fsds" {
  name     = "fsds"
  instance = google_sql_database_instance.primary.name
  password = random_password.pg_password.result
}
