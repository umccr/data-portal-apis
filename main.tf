// actual terraform stack is in umccr/infrastructure repo
// this is just to get terrafrom output from remote state
terraform {
  required_version = "~> 0.11.14"

  backend "s3" {
    bucket = "umccr-terraform-states"
    key    = "umccr_data_portal/terraform.tfstate"
    region = "ap-southeast-2"
  }
}
