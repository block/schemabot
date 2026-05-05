output "service_url" {
  description = "SchemaBot API URL"
  value       = "https://${aws_apprunner_service.schemabot.service_url}"
}

output "webhook_url" {
  description = "GitHub App webhook URL (use this when creating the GitHub App)"
  value       = "https://${aws_apprunner_service.schemabot.service_url}/webhook"
}

output "ecr_repository_url" {
  description = "ECR repository URL for Docker images"
  value       = aws_ecr_repository.schemabot.repository_url
}

output "testapp_endpoint" {
  description = "Testapp RDS endpoint"
  value       = aws_db_instance.testapp.endpoint
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

output "bastion_ssm" {
  description = "SSM command to port-forward to target RDS"
  value       = "aws ssm start-session --target ${aws_instance.bastion.id} --document-name AWS-StartPortForwardingSessionToRemoteHost --parameters '{\"host\":[\"${aws_db_instance.testapp.address}\"],\"portNumber\":[\"3306\"],\"localPortNumber\":[\"13372\"]}'"
}

output "seed_commands" {
  description = "Commands to seed testapp database via bastion"
  sensitive   = true
  value       = <<-EOF

    # Terminal 1: Port forward to ${var.environment}
    ${aws_instance.bastion.id != "" ? "aws ssm start-session --target ${aws_instance.bastion.id} --document-name AWS-StartPortForwardingSessionToRemoteHost --parameters '{\"host\":[\"${aws_db_instance.testapp.address}\"],\"portNumber\":[\"3306\"],\"localPortNumber\":[\"13372\"]}'" : ""}

    # Terminal 2: Seed database
    mysql -h 127.0.0.1 -P 13372 -u testapp -p'${random_password.testapp.result}' testapp < seed.sql

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

output "testapp_dsn" {
  description = "DSN for testapp database (for reference)"
  sensitive   = true
  value       = "testapp:${random_password.testapp.result}@tcp(${aws_db_instance.testapp.endpoint})/testapp?parseTime=true&tls=true"
}

output "github_app_secret_id" {
  description = "Secrets Manager secret ID for GitHub App credentials"
  value       = aws_secretsmanager_secret.github_app.name
}

output "storage_dsn_secret_id" {
  description = "Secrets Manager secret ID for storage DSN"
  value       = aws_secretsmanager_secret.storage_dsn.name
}

output "testapp_dsn_secret_id" {
  description = "Secrets Manager secret ID for testapp DSN"
  value       = aws_secretsmanager_secret.testapp_dsn.name
}

output "apprunner_service_arn" {
  description = "App Runner service ARN (for deployments)"
  value       = aws_apprunner_service.schemabot.arn
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

    allowed_environments:
      - ${var.environment}
    respond_to_unscoped: false

    databases:
      testapp:
        type: mysql
        environments:
          ${var.environment}:
            dsn: "secretsmanager:${aws_secretsmanager_secret.testapp_dsn.name}#dsn"
  EOF
}
