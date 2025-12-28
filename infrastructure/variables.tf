# Variables for Soccer Scraper Infrastructure
# All variables follow snake_case naming convention

# AWS Region for resource deployment
variable "aws_region" {
  description = "AWS region where resources will be deployed"
  type        = string
  default     = "us-west-2"
  
  validation {
    condition     = can(regex("^[a-z]{2}-[a-z]+-[0-9]$", var.aws_region))
    error_message = "AWS region must be in format: us-west-2, us-east-1, etc."
  }
}

# Environment identifier (dev, staging, prod)
variable "environment" {
  description = "Environment name for resource tagging and naming"
  type        = string
  default     = "prod"
  
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod"
  }
}

# Lambda function name
variable "lambda_function_name" {
  description = "Name of the Lambda function for soccer schedule scraping"
  type        = string
  default     = "soccer_schedule_scraper"
}

# DynamoDB table name for schedule storage
variable "schedule_table_name" {
  description = "DynamoDB table name for storing historical schedules"
  type        = string
  default     = "soccer_schedules"
}

# SNS topic name for notifications
variable "sns_topic_name" {
  description = "SNS topic name for schedule change notifications"
  type        = string
  default     = "soccer_schedule_changes"
}

# Schedule check frequency in minutes
variable "schedule_check_frequency" {
  description = "How often to check for schedule changes (in minutes)"
  type        = number
  default     = 60
  
  validation {
    condition     = var.schedule_check_frequency >= 1 && var.schedule_check_frequency <= 1440
    error_message = "Check frequency must be between 1 and 1440 minutes (24 hours)"
  }
}

# Feature flag for enabling scheduled checks
variable "enable_scheduled_checks" {
  description = "Enable automated schedule change checking via EventBridge"
  type        = bool
  default     = true
}
