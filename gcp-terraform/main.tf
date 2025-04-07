module "composer" {
  source     = "./modules/composer"
  project_id = var.project_id
  region     = var.region
  composer_environment_name = var.composer_environment_name
}


module "superset" {
  source     = "./modules/superset"
  project_id = var.project_id
  region     = var.region
}