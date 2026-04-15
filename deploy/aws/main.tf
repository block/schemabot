# SchemaBot Example Deployment
#
# Minimal AWS deployment demonstrating SchemaBot with sample target databases.
# Uses cheap RDS instances suitable for demos and testing.
#
# Resources created:
#   - SchemaBot on App Runner
#   - RDS MySQL for SchemaBot storage (db.t4g.micro)
#   - RDS MySQL for testapp staging (db.t4g.micro)
#   - RDS MySQL for testapp production (db.t4g.micro)
#   - NAT/Bastion instance for RDS access (t4g.nano, ~$3/month)
#
# Estimated cost: ~$35-55/month (can be stopped when not in use)
#
# Usage:
#   1. terraform init
#   2. terraform apply
#   3. Push Docker image (see outputs)
#   4. Seed data via bastion (see outputs)
#   5. Test with CLI

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
  region  = var.aws_region
  profile = var.aws_profile != "" ? var.aws_profile : null
}

# -----------------------------------------------------------------------------
# Variables
# -----------------------------------------------------------------------------

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-west-2"
}

variable "aws_profile" {
  description = "AWS CLI profile (optional)"
  type        = string
  default     = ""
}

variable "name_prefix" {
  description = "Prefix for all resource names"
  type        = string
  default     = "schemabot-example"
}


locals {
  tags = {
    Project   = "schemabot-example"
    ManagedBy = "terraform"
  }
}

# -----------------------------------------------------------------------------
# Data Sources - Use default VPC for simplicity
# -----------------------------------------------------------------------------

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }

  filter {
    name   = "default-for-az"
    values = ["true"]
  }
}

data "aws_availability_zones" "available" {
  state = "available"
}

# -----------------------------------------------------------------------------
# Security Groups
# -----------------------------------------------------------------------------

resource "aws_security_group" "rds" {
  name        = "${var.name_prefix}-rds"
  description = "Allow MySQL from App Runner and Bastion"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    from_port       = 3306
    to_port         = 3306
    protocol        = "tcp"
    security_groups = [aws_security_group.apprunner_private.id]
    description     = "MySQL from App Runner"
  }

  ingress {
    from_port       = 3306
    to_port         = 3306
    protocol        = "tcp"
    security_groups = [aws_security_group.bastion.id]
    description     = "MySQL from Bastion"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.tags, { Name = "${var.name_prefix}-rds" })
}

resource "aws_security_group" "apprunner" {
  name        = "${var.name_prefix}-apprunner"
  description = "App Runner VPC connector"
  vpc_id      = data.aws_vpc.default.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.tags, { Name = "${var.name_prefix}-apprunner" })
}

resource "aws_security_group" "bastion" {
  name        = "${var.name_prefix}-bastion"
  description = "Bastion for RDS access via SSM"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["172.31.64.0/23"] # Both private subnets
    description = "All traffic from private subnets (NAT)"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.tags, { Name = "${var.name_prefix}-bastion" })
}

# -----------------------------------------------------------------------------
# NAT/Bastion Instance (for RDS access via SSM + internet egress for App Runner)
# Uses fck-nat AMI - pre-configured NAT with SSM agent (~$3/month)
# -----------------------------------------------------------------------------

# fck-nat AMI - reliable, minimal NAT/bastion image
data "aws_ami" "fck_nat" {
  most_recent = true
  owners      = ["568608671756"] # fck-nat AMI owner

  filter {
    name   = "name"
    values = ["fck-nat-al2023-*-arm64-ebs"]
  }

  filter {
    name   = "architecture"
    values = ["arm64"]
  }
}

resource "aws_iam_role" "bastion" {
  name = "${var.name_prefix}-bastion"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })

  tags = local.tags
}

resource "aws_iam_role_policy_attachment" "bastion_ssm" {
  role       = aws_iam_role.bastion.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "bastion" {
  name = "${var.name_prefix}-bastion"
  role = aws_iam_role.bastion.name

  tags = local.tags
}

resource "aws_instance" "bastion" {
  ami                    = data.aws_ami.fck_nat.id
  instance_type          = "t4g.nano" # ~$3/month
  subnet_id              = data.aws_subnets.default.ids[0]
  vpc_security_group_ids = [aws_security_group.bastion.id]
  iam_instance_profile   = aws_iam_instance_profile.bastion.name

  source_dest_check           = false # Required for NAT
  associate_public_ip_address = true

  # fck-nat AMI is stripped down — SSM agent needs to be installed for port forwarding
  user_data = <<-EOF
    #!/bin/bash
    dnf install -y amazon-ssm-agent
    systemctl enable amazon-ssm-agent
    systemctl start amazon-ssm-agent
  EOF

  metadata_options {
    http_endpoint = "enabled"
    http_tokens   = "required" # IMDSv2 only
  }

  root_block_device {
    volume_type           = "gp3"
    volume_size           = 8
    encrypted             = true
    delete_on_termination = true
  }

  tags = merge(local.tags, { Name = "${var.name_prefix}-nat-bastion" })

  lifecycle {
    ignore_changes = [ami]
  }
}

# -----------------------------------------------------------------------------
# Private Subnets (for App Runner VPC connector)
# App Runner ENIs don't get public IPs, so they need private subnets
# routed through the NAT instance for internet access (GitHub API, etc.)
# Using 172.31.64.0/24 and 172.31.65.0/24 (unused in default VPC)
# -----------------------------------------------------------------------------

resource "aws_subnet" "private" {
  count             = 2
  vpc_id            = data.aws_vpc.default.id
  cidr_block        = "172.31.${64 + count.index}.0/24"
  availability_zone = data.aws_availability_zones.available.names[count.index]

  tags = merge(local.tags, { Name = "${var.name_prefix}-private-${count.index + 1}" })
}

resource "aws_route_table" "private" {
  vpc_id = data.aws_vpc.default.id

  route {
    cidr_block           = "0.0.0.0/0"
    network_interface_id = aws_instance.bastion.primary_network_interface_id
  }

  tags = merge(local.tags, { Name = "${var.name_prefix}-private-rt" })
}

resource "aws_route_table_association" "private" {
  count          = 2
  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}

# -----------------------------------------------------------------------------
# RDS Subnet Group
# -----------------------------------------------------------------------------

resource "aws_db_subnet_group" "main" {
  name       = var.name_prefix
  subnet_ids = data.aws_subnets.default.ids

  tags = merge(local.tags, { Name = var.name_prefix })
}

# -----------------------------------------------------------------------------
# Passwords
# -----------------------------------------------------------------------------

resource "random_password" "schemabot_db" {
  length  = 24
  special = false
}

resource "random_password" "testapp_staging" {
  length  = 24
  special = false
}

resource "random_password" "testapp_production" {
  length  = 24
  special = false
}

# -----------------------------------------------------------------------------
# RDS Parameter Group - Spirit requires binlog, performance_schema
# -----------------------------------------------------------------------------

resource "aws_db_parameter_group" "schemabot" {
  name   = "${var.name_prefix}-mysql84"
  family = "mysql8.4"

  parameter {
    name  = "binlog_format"
    value = "ROW"
  }

  parameter {
    name  = "binlog_row_image"
    value = "FULL"
  }

  parameter {
    name  = "performance_schema"
    value = "1"
    apply_method = "pending-reboot"
  }

  # Automatically activate all granted roles on login. Required because Spirit
  # doesn't yet detect privileges granted via roles like rds_superuser_role.
  parameter {
    name  = "activate_all_roles_on_login"
    value = "1"
  }

  tags = local.tags
}

# -----------------------------------------------------------------------------
# RDS Instances
# -----------------------------------------------------------------------------

# SchemaBot's own storage
resource "aws_db_instance" "schemabot" {
  identifier = "${var.name_prefix}-storage"

  engine                      = "mysql"
  engine_version              = "8.4"
  instance_class              = "db.t4g.micro"
  allow_major_version_upgrade = true
  apply_immediately           = true

  allocated_storage = 20
  storage_type      = "gp2"

  db_name  = "schemabot"
  username = "schemabot"
  password = random_password.schemabot_db.result

  parameter_group_name   = aws_db_parameter_group.schemabot.name
  db_subnet_group_name   = aws_db_subnet_group.main.name
  vpc_security_group_ids = [aws_security_group.rds.id]
  publicly_accessible    = false

  # backup_retention_period >= 1 enables binary logging (required by Spirit)
  backup_retention_period  = 1
  skip_final_snapshot      = true
  deletion_protection      = false
  copy_tags_to_snapshot    = true

  tags = merge(local.tags, { Name = "${var.name_prefix}-storage" })
}

# Testapp staging database
resource "aws_db_instance" "testapp_staging" {
  identifier = "${var.name_prefix}-testapp-staging"

  engine                      = "mysql"
  engine_version              = "8.4"
  instance_class              = "db.t4g.micro"
  allow_major_version_upgrade = true
  apply_immediately           = true

  allocated_storage = 20
  storage_type      = "gp2"

  db_name  = "testapp"
  username = "testapp"
  password = random_password.testapp_staging.result

  parameter_group_name   = aws_db_parameter_group.schemabot.name
  db_subnet_group_name   = aws_db_subnet_group.main.name
  vpc_security_group_ids = [aws_security_group.rds.id]
  publicly_accessible    = false

  backup_retention_period  = 1
  skip_final_snapshot      = true
  deletion_protection      = false
  copy_tags_to_snapshot    = true

  tags = merge(local.tags, { Name = "${var.name_prefix}-testapp-staging" })
}

# Testapp production database
resource "aws_db_instance" "testapp_production" {
  identifier = "${var.name_prefix}-testapp-production"

  engine                      = "mysql"
  engine_version              = "8.4"
  instance_class              = "db.t4g.micro"
  allow_major_version_upgrade = true
  apply_immediately           = true

  allocated_storage = 20
  storage_type      = "gp2"

  db_name  = "testapp"
  username = "testapp"
  password = random_password.testapp_production.result

  parameter_group_name   = aws_db_parameter_group.schemabot.name
  db_subnet_group_name   = aws_db_subnet_group.main.name
  vpc_security_group_ids = [aws_security_group.rds.id]
  publicly_accessible    = false

  backup_retention_period  = 1
  skip_final_snapshot      = true
  deletion_protection      = false
  copy_tags_to_snapshot    = true

  tags = merge(local.tags, { Name = "${var.name_prefix}-testapp-production" })
}

# -----------------------------------------------------------------------------
# Secrets Manager - Store DSNs securely
# -----------------------------------------------------------------------------

resource "aws_secretsmanager_secret" "storage_dsn" {
  name                    = "${var.name_prefix}/storage-dsn"
  description             = "SchemaBot internal storage database DSN"
  recovery_window_in_days = 0
  tags                    = local.tags
}

resource "aws_secretsmanager_secret_version" "storage_dsn" {
  secret_id = aws_secretsmanager_secret.storage_dsn.id
  secret_string = jsonencode({
    dsn = "${aws_db_instance.schemabot.username}:${random_password.schemabot_db.result}@tcp(${aws_db_instance.schemabot.endpoint})/${aws_db_instance.schemabot.db_name}?parseTime=true&multiStatements=true&tls=true"
  })

  # DSN is updated by bootstrap.sh to use the dedicated Spirit user
  lifecycle {
    ignore_changes = [secret_string]
  }
}

resource "aws_secretsmanager_secret" "testapp_staging_dsn" {
  name                    = "${var.name_prefix}/testapp-staging"
  description             = "Testapp staging database DSN"
  recovery_window_in_days = 0
  tags                    = local.tags
}

resource "aws_secretsmanager_secret_version" "testapp_staging_dsn" {
  secret_id = aws_secretsmanager_secret.testapp_staging_dsn.id
  secret_string = jsonencode({
    dsn = "testapp:${random_password.testapp_staging.result}@tcp(${aws_db_instance.testapp_staging.endpoint})/testapp?parseTime=true&tls=true"
  })

  # DSN is updated by bootstrap.sh to use the dedicated Spirit user
  lifecycle {
    ignore_changes = [secret_string]
  }
}

resource "aws_secretsmanager_secret" "testapp_production_dsn" {
  name                    = "${var.name_prefix}/testapp-production"
  description             = "Testapp production database DSN"
  recovery_window_in_days = 0
  tags                    = local.tags
}

resource "aws_secretsmanager_secret_version" "testapp_production_dsn" {
  secret_id = aws_secretsmanager_secret.testapp_production_dsn.id
  secret_string = jsonencode({
    dsn = "testapp:${random_password.testapp_production.result}@tcp(${aws_db_instance.testapp_production.endpoint})/testapp?parseTime=true&tls=true"
  })

  # DSN is updated by bootstrap.sh to use the dedicated Spirit user
  lifecycle {
    ignore_changes = [secret_string]
  }
}

# GitHub App credentials (populated after creating the GitHub App)
resource "aws_secretsmanager_secret" "github_app" {
  name                    = "${var.name_prefix}/github-app"
  description             = "GitHub App credentials for SchemaBot webhook integration"
  recovery_window_in_days = 0
  tags                    = local.tags
}

resource "aws_secretsmanager_secret_version" "github_app" {
  secret_id = aws_secretsmanager_secret.github_app.id
  secret_string = jsonencode({
    "app-id"         = "0"
    "private-key"    = ""
    "webhook-secret" = ""
  })

  # Credentials are managed by setup-github-app.sh via AWS CLI, not Terraform
  lifecycle {
    ignore_changes = [secret_string]
  }
}

# -----------------------------------------------------------------------------
# ECR Repository
# -----------------------------------------------------------------------------

resource "aws_ecr_repository" "schemabot" {
  name                 = var.name_prefix
  image_tag_mutability = "MUTABLE"
  force_delete         = true

  tags = local.tags
}

# -----------------------------------------------------------------------------
# IAM Roles for App Runner
# -----------------------------------------------------------------------------

resource "aws_iam_role" "apprunner_ecr" {
  name = "${var.name_prefix}-apprunner-ecr"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "build.apprunner.amazonaws.com" }
    }]
  })

  tags = local.tags
}

resource "aws_iam_role_policy_attachment" "apprunner_ecr" {
  role       = aws_iam_role.apprunner_ecr.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSAppRunnerServicePolicyForECRAccess"
}

resource "aws_iam_role" "apprunner_instance" {
  name = "${var.name_prefix}-apprunner-instance"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "tasks.apprunner.amazonaws.com" }
    }]
  })

  tags = local.tags
}

# Allow App Runner to read secrets
resource "aws_iam_role_policy" "apprunner_secrets" {
  name = "secrets-access"
  role = aws_iam_role.apprunner_instance.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = ["secretsmanager:GetSecretValue"]
      Resource = [
        "arn:aws:secretsmanager:*:*:secret:${var.name_prefix}/*"
      ]
    }]
  })
}

# -----------------------------------------------------------------------------
# VPC Endpoints (for AWS service access from VPC)
# -----------------------------------------------------------------------------

# Security group for VPC endpoints
resource "aws_security_group" "vpc_endpoints" {
  name        = "${var.name_prefix}-vpc-endpoints"
  description = "Security group for VPC endpoints"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description     = "HTTPS from App Runner"
    from_port       = 443
    to_port         = 443
    protocol        = "tcp"
    security_groups = [aws_security_group.apprunner_private.id]
  }

  tags = merge(local.tags, {
    Name = "${var.name_prefix}-vpc-endpoints"
  })
}

# Secrets Manager endpoint - allows App Runner to fetch DSNs
resource "aws_vpc_endpoint" "secretsmanager" {
  vpc_id              = data.aws_vpc.default.id
  service_name        = "com.amazonaws.${var.aws_region}.secretsmanager"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = aws_subnet.private[*].id
  security_group_ids  = [aws_security_group.vpc_endpoints.id]
  private_dns_enabled = true

  tags = merge(local.tags, {
    Name = "${var.name_prefix}-secretsmanager"
  })
}

# -----------------------------------------------------------------------------
# App Runner VPC Connector
# -----------------------------------------------------------------------------

resource "aws_security_group" "apprunner_private" {
  name        = "${var.name_prefix}-apprunner-private"
  description = "App Runner VPC connector in private subnets"
  vpc_id      = data.aws_vpc.default.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.tags, { Name = "${var.name_prefix}-apprunner-private" })
}

resource "aws_apprunner_vpc_connector" "main" {
  vpc_connector_name = var.name_prefix
  subnets            = aws_subnet.private[*].id
  security_groups    = [aws_security_group.apprunner_private.id]

  tags = local.tags

  lifecycle {
    create_before_destroy = true
  }
}

# -----------------------------------------------------------------------------
# App Runner Service
# -----------------------------------------------------------------------------

resource "aws_apprunner_service" "schemabot" {
  service_name = var.name_prefix

  source_configuration {
    auto_deployments_enabled = false

    image_repository {
      image_configuration {
        port = "8080"
        runtime_environment_variables = {
          PORT                  = "8080"
          LOG_LEVEL             = "debug"
          AWS_REGION            = var.aws_region
          SCHEMABOT_CONFIG_FILE = "/config/aws-example.yaml"
        }
      }
      image_identifier      = "${aws_ecr_repository.schemabot.repository_url}:latest"
      image_repository_type = "ECR"
    }

    authentication_configuration {
      access_role_arn = aws_iam_role.apprunner_ecr.arn
    }
  }

  instance_configuration {
    cpu               = "0.25 vCPU"
    memory            = "0.5 GB"
    instance_role_arn = aws_iam_role.apprunner_instance.arn
  }

  network_configuration {
    egress_configuration {
      egress_type       = "VPC"
      vpc_connector_arn = aws_apprunner_vpc_connector.main.arn
    }
  }

  health_check_configuration {
    healthy_threshold   = 1
    interval            = 20
    path                = "/health"
    protocol            = "HTTP"
    timeout             = 10
    unhealthy_threshold = 3
  }

  tags = local.tags

  depends_on = [aws_db_instance.schemabot]
}

# -----------------------------------------------------------------------------
# Outputs
# -----------------------------------------------------------------------------

output "service_url" {
  description = "SchemaBot API URL"
  value       = "https://${aws_apprunner_service.schemabot.service_url}"
}

output "webhook_url" {
  description = "GitHub App webhook URL (use this when creating the GitHub App)"
  value       = "https://${aws_apprunner_service.schemabot.service_url}/webhook"
}

output "ecr_repository_url" {
  value = aws_ecr_repository.schemabot.repository_url
}

output "testapp_staging_endpoint" {
  value = aws_db_instance.testapp_staging.endpoint
}

output "testapp_production_endpoint" {
  value = aws_db_instance.testapp_production.endpoint
}

output "schemabot_endpoint" {
  description = "SchemaBot internal storage database endpoint"
  value       = aws_db_instance.schemabot.endpoint
}

output "schemabot_dsn" {
  description = "SchemaBot internal storage database DSN"
  sensitive   = true
  value       = "${aws_db_instance.schemabot.username}:${random_password.schemabot_db.result}@tcp(${aws_db_instance.schemabot.endpoint})/${aws_db_instance.schemabot.db_name}?parseTime=true&tls=true"
}

output "bastion_instance_id" {
  description = "Bastion instance ID for SSM sessions"
  value       = aws_instance.bastion.id
}

output "bastion_ssm_staging" {
  description = "SSM command to port-forward to staging RDS"
  value       = "aws ssm start-session --target ${aws_instance.bastion.id} --document-name AWS-StartPortForwardingSessionToRemoteHost --parameters '{\"host\":[\"${aws_db_instance.testapp_staging.address}\"],\"portNumber\":[\"3306\"],\"localPortNumber\":[\"13372\"]}'"
}

output "bastion_ssm_production" {
  description = "SSM command to port-forward to production RDS"
  value       = "aws ssm start-session --target ${aws_instance.bastion.id} --document-name AWS-StartPortForwardingSessionToRemoteHost --parameters '{\"host\":[\"${aws_db_instance.testapp_production.address}\"],\"portNumber\":[\"3306\"],\"localPortNumber\":[\"13373\"]}'"
}

output "seed_commands" {
  description = "Commands to seed testapp databases via bastion"
  sensitive   = true
  value       = <<-EOF

    # Seed data uses SSM port forwarding (same ports as local dev: 13372, 13373)
    # Run each SSM session in a separate terminal, then seed:

    # Terminal 1: Port forward to staging
    ${aws_instance.bastion.id != "" ? "aws ssm start-session --target ${aws_instance.bastion.id} --document-name AWS-StartPortForwardingSessionToRemoteHost --parameters '{\"host\":[\"${aws_db_instance.testapp_staging.address}\"],\"portNumber\":[\"3306\"],\"localPortNumber\":[\"13372\"]}'" : ""}

    # Terminal 2: Port forward to production
    ${aws_instance.bastion.id != "" ? "aws ssm start-session --target ${aws_instance.bastion.id} --document-name AWS-StartPortForwardingSessionToRemoteHost --parameters '{\"host\":[\"${aws_db_instance.testapp_production.address}\"],\"portNumber\":[\"3306\"],\"localPortNumber\":[\"13373\"]}'" : ""}

    # Terminal 3: Seed both databases
    mysql -h 127.0.0.1 -P 13372 -u testapp -p'${random_password.testapp_staging.result}' testapp < seed.sql
    mysql -h 127.0.0.1 -P 13373 -u testapp -p'${random_password.testapp_production.result}' testapp < seed.sql

  EOF
}

output "deploy_commands" {
  description = "Commands to build and deploy"
  value       = <<-EOF

    # 1. Login to ECR
    aws ecr get-login-password --region ${var.aws_region} | docker login --username AWS --password-stdin ${aws_ecr_repository.schemabot.repository_url}

    # 2. Build and push image
    docker build -t ${aws_ecr_repository.schemabot.repository_url}:latest -f deploy/Dockerfile .
    docker push ${aws_ecr_repository.schemabot.repository_url}:latest

    # 3. Trigger deployment
    aws apprunner start-deployment --service-arn ${aws_apprunner_service.schemabot.arn}

    # 4. Test health
    curl https://${aws_apprunner_service.schemabot.service_url}/health

  EOF
}

output "testapp_dsns" {
  description = "DSNs for testapp databases (for reference)"
  sensitive   = true
  value = {
    staging    = "testapp:${random_password.testapp_staging.result}@tcp(${aws_db_instance.testapp_staging.endpoint})/testapp?parseTime=true&tls=true"
    production = "testapp:${random_password.testapp_production.result}@tcp(${aws_db_instance.testapp_production.endpoint})/testapp?parseTime=true&tls=true"
  }
}

output "github_app_secret_id" {
  description = "Secrets Manager secret ID for GitHub App credentials"
  value       = aws_secretsmanager_secret.github_app.name
}

output "storage_dsn_secret_id" {
  description = "Secrets Manager secret ID for storage DSN"
  value       = aws_secretsmanager_secret.storage_dsn.name
}

output "testapp_staging_dsn_secret_id" {
  description = "Secrets Manager secret ID for testapp staging DSN"
  value       = aws_secretsmanager_secret.testapp_staging_dsn.name
}

output "testapp_production_dsn_secret_id" {
  description = "Secrets Manager secret ID for testapp production DSN"
  value       = aws_secretsmanager_secret.testapp_production_dsn.name
}

output "config_yaml" {
  description = "SchemaBot config to embed in Docker image"
  value       = <<-EOF
    # Save this as deploy/aws/config.yaml
    # Secrets are fetched from AWS Secrets Manager at runtime
    storage:
      dsn: "secretsmanager:${aws_secretsmanager_secret.storage_dsn.name}#dsn"

    github:
      app-id: "secretsmanager:${aws_secretsmanager_secret.github_app.name}#app-id"
      private-key: "secretsmanager:${aws_secretsmanager_secret.github_app.name}#private-key"
      webhook-secret: "secretsmanager:${aws_secretsmanager_secret.github_app.name}#webhook-secret"

    databases:
      testapp:
        type: mysql
        environments:
          staging:
            dsn: "secretsmanager:${aws_secretsmanager_secret.testapp_staging_dsn.name}#dsn"
          production:
            dsn: "secretsmanager:${aws_secretsmanager_secret.testapp_production_dsn.name}#dsn"
  EOF
}
