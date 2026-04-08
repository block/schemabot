terraform {
  backend "s3" {
    # Bucket and lock table are passed via -backend-config at init time.
    # bootstrap.sh creates these resources and runs:
    #   terraform init \
    #     -backend-config="bucket=schemabot-terraform-state-${ACCOUNT_ID}" \
    #     -backend-config="dynamodb_table=schemabot-terraform-lock"
    key     = "deploy/aws/terraform.tfstate"
    region  = "us-west-2"
    encrypt = true
  }
}
