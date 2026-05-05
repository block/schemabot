terraform {
  backend "s3" {
    key     = "deploy/aws-multi-env/staging/terraform.tfstate"
    region  = "us-west-2"
    encrypt = true
  }
}
