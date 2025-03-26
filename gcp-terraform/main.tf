provider "google-beta" {
  project = "goorm-bomnet"
  region  = "asia-northeast3"
}

resource "google_project_service" "composer_api" {
  provider = google-beta
  project = "goorm-bomnet"
  service = "composer.googleapis.com"
  // Disabling Cloud Composer API might irreversibly break all other
  // environments in your project.
  // This parameter prevents automatic disabling
  // of the API when the resource is destroyed.
  // We recommend to disable the API only after all environments are deleted.
  disable_on_destroy = false
  // this flag is introduced in 5.39.0 version of Terraform. If set to true it will
  //prevent you from disabling composer_api through Terraform if any environment was
  //there in the last 30 days
  check_if_service_has_usage_on_destroy = true
}

# 1️⃣ 사용자 관리 서비스 계정 생성
resource "google_service_account" "composer_sa" {
  provider     = google-beta
  account_id   = "composer-admin-sa"
  display_name = "Cloud Composer DAG Admin Service Account"
  depends_on = [google_project_service.composer_api]
}

# 2️⃣ Cloud Composer 환경 운영을 위한 역할
resource "google_project_iam_member" "composer_worker_role" {
  provider = google-beta
  project  = "goorm-bomnet"
  member   = format("serviceAccount:%s", google_service_account.composer_sa.email)
  role     = "roles/composer.worker"
}

# 3️⃣ BigQuery Admin 권한 (모든 데이터셋 및 테이블 관리)
resource "google_project_iam_member" "bigquery_admin" {
  provider = google-beta
  project  = "goorm-bomnet"
  member   = format("serviceAccount:%s", google_service_account.composer_sa.email)
  role     = "roles/bigquery.admin"
}

# 4️⃣ Cloud Storage (GCS) Admin 권한 (모든 버킷 및 객체 관리)
resource "google_project_iam_member" "gcs_admin" {
  provider = google-beta
  project  = "goorm-bomnet"
  member   = format("serviceAccount:%s", google_service_account.composer_sa.email)
  role     = "roles/storage.admin"
}

# 5️⃣ Pub/Sub Admin 권한 (모든 토픽 및 구독 관리)
resource "google_project_iam_member" "pubsub_admin" {
  provider = google-beta
  project  = "goorm-bomnet"
  member   = format("serviceAccount:%s", google_service_account.composer_sa.email)
  role     = "roles/pubsub.admin"
}

resource "google_composer_environment" "goorm_environment" {
  provider = google-beta
  name = "goorm-environment"

  config {

    workloads_config {
      scheduler {

      }

      worker {
        min_count = 1
        max_count = 4
        cpu = 4
        memory_gb = 8
        storage_gb = 15
      }
    }

    software_config {
      image_version = "composer-3-airflow-2.10.2-build.11"

      pypi_packages = {
        "astronomer-cosmos" = ">=1.9"
      }
    }
    node_config {
      service_account = google_service_account.composer_sa.email
    }
  }
}