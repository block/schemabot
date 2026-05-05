variable "name_prefix" {
  description = "Prefix for all resource names (e.g. schemabot-staging)"
  type        = string
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-west-2"
}

variable "environment" {
  description = "Environment name (staging or production)"
  type        = string

  validation {
    condition     = contains(["staging", "production"], var.environment)
    error_message = "environment must be 'staging' or 'production'"
  }
}

variable "config_file_path" {
  description = "Path to SchemaBot config file inside the container (e.g. /config/aws-multi-env-staging.yaml)"
  type        = string
}

variable "subnet_offset" {
  description = "Starting octet for private subnets (e.g. 64 for 172.31.64.0/24 and 172.31.65.0/24)"
  type        = number
}
