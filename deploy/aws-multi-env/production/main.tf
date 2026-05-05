terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
  }
}

provider "aws" {
  region  = "us-west-2"
  profile = var.aws_profile != "" ? var.aws_profile : null
}

variable "aws_profile" {
  description = "AWS CLI profile (optional)"
  type        = string
  default     = ""
}

module "schemabot" {
  source           = "../modules/schemabot"
  name_prefix      = "schemabot-production"
  environment      = "production"
  config_file_path = "/config/aws-multi-env-production.yaml"
  subnet_offset    = 66
}

# Pass through all module outputs

output "service_url" {
  value = module.schemabot.service_url
}

output "webhook_url" {
  value = module.schemabot.webhook_url
}

output "ecr_repository_url" {
  value = module.schemabot.ecr_repository_url
}

output "testapp_production_endpoint" {
  value = module.schemabot.testapp_endpoint
}

output "schemabot_endpoint" {
  description = "SchemaBot internal storage database endpoint"
  value       = module.schemabot.schemabot_endpoint
}

output "schemabot_dsn" {
  description = "SchemaBot internal storage database DSN"
  sensitive   = true
  value       = module.schemabot.schemabot_dsn
}

output "bastion_instance_id" {
  description = "Bastion instance ID for SSM sessions"
  value       = module.schemabot.bastion_instance_id
}

output "bastion_ssm_production" {
  description = "SSM command to port-forward to production RDS"
  value       = module.schemabot.bastion_ssm
}

output "seed_commands" {
  description = "Commands to seed testapp production database via bastion"
  sensitive   = true
  value       = module.schemabot.seed_commands
}

output "deploy_commands" {
  description = "Commands to build and deploy"
  value       = module.schemabot.deploy_commands
}

output "testapp_dsns" {
  description = "DSNs for testapp databases (for reference)"
  sensitive   = true
  value = {
    production = module.schemabot.testapp_dsn
  }
}

output "github_app_secret_id" {
  description = "Secrets Manager secret ID for GitHub App credentials"
  value       = module.schemabot.github_app_secret_id
}

output "storage_dsn_secret_id" {
  description = "Secrets Manager secret ID for storage DSN"
  value       = module.schemabot.storage_dsn_secret_id
}

output "testapp_production_dsn_secret_id" {
  description = "Secrets Manager secret ID for testapp production DSN"
  value       = module.schemabot.testapp_dsn_secret_id
}

output "config_yaml" {
  description = "SchemaBot config to embed in Docker image"
  value       = module.schemabot.config_yaml
}
