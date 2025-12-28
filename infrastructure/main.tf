# Main OpenTofu configuration for Soccer Scraper Infrastructure
# This file configures the AWS provider and backend for state management

terraform {
  required_version = ">= 1.6.0"
  
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  
  # Backend configuration for remote state storage
  # This can be configured via CLI or backend config file
  backend "s3" {
    # bucket = "configured-via-cli"
    # key    = "soccer-scraper/terraform.tfstate"
    # region = "us-west-2"
  }
}

# AWS Provider configuration
provider "aws" {
  region = var.aws_region
  
  default_tags {
    tags = {
      Project     = "SoccerScraper"
      ManagedBy   = "OpenTofu"
      Environment = var.environment
    }
  }
}
