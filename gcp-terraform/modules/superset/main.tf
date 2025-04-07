resource "google_compute_network" "vpc" {
  name                    = "superset-vpc"
  auto_create_subnetworks = false
  project                 = var.project_id
}

resource "google_compute_subnetwork" "subnet" {
  name          = "superset-subnet"
  region        = var.region
  network       = google_compute_network.vpc.id
  ip_cidr_range = "10.10.0.0/24"
  project       = var.project_id
}

resource "google_compute_firewall" "allow-nodeport" {
  name    = "allow-nodeport"
  network = google_compute_network.vpc.name
  project = var.project_id

  allow {
    protocol = "tcp"
    ports    = ["22", "80", "443", "30000-32767"]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["superset-node"]
}

resource "google_compute_instance" "vm" {
  name         = "superset-vm"
  machine_type = "e2-standard-2"
  project      = var.project_id
  zone =""
  tags = ["superset-node"]

  boot_disk {
    initialize_params {
      image = "projects/ubuntu-os-cloud/global/images/family/ubuntu-2204-lts"
      size  = 50
    }
  }

  network_interface {
    subnetwork   = google_compute_subnetwork.subnet.name
    access_config {}
  }

  metadata_startup_script = file("${path.module}/../../scripts/install_minikube.sh")
}
