# creates resources from the modules (core services & ducklake services) 
module "core_services" {
  source                = "./core_services"
  AWS_ACCESS_KEY_ID     = var.AWS_ACCESS_KEY_ID
  AWS_SECRET_ACCESS_KEY = var.AWS_SECRET_ACCESS_KEY
  AWS_DEFAULT_REGION    = var.AWS_DEFAULT_REGION
}

module "ducklake_services" {
  source             = "./ducklake_services"
  aws_region         = var.AWS_DEFAULT_REGION
  vpc_id             = module.core_services.vpc_id
  subnet_ids         = module.core_services.public_subnet_ids
  bucket_names       = ["dmc93-ducklake-dev", "dmc93-ducklake-test", "dmc93-ducklake-prod"]
  db_username        = var.db_username
  db_password        = var.db_password
  bastion_ingress_ip = var.bastion_ingress_ip
}

