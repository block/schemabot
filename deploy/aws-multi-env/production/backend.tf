terraform {
  backend "s3" {
    key     = "deploy/aws-multi-env/production/terraform.tfstate"
    region  = "us-west-2"
    encrypt = true
  }
}
