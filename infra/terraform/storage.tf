# Two buckets, not one shared bucket with two prefixes -- mirrors the
# current pipeline_config.yaml `layers.bronze/silver` shape (bucket + prefix
# each) and keeps IAM scoping clean: GCS IAM is bucket-level, so one bucket
# with two prefixes couldn't grant Bronze/Silver access separately if that
# ever mattered.
resource "google_storage_bucket" "bronze" {
  name                        = "fsds-ecommerce-bronze-data"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true
}

resource "google_storage_bucket" "silver" {
  name                        = "fsds-ecommerce-silver-data"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true
}

