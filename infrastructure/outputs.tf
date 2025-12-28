# Outputs for Soccer Scraper Infrastructure
# Exports important resource identifiers and URLs

output "lambda_function_name" {
  description = "Name of the Lambda function"
  value       = aws_lambda_function.scraper.function_name
}

output "lambda_function_arn" {
  description = "ARN of the Lambda function"
  value       = aws_lambda_function.scraper.arn
}

output "lambda_function_url" {
  description = "URL for accessing the Lambda function"
  value       = aws_lambda_function_url.scraper_url.function_url
}

output "sns_topic_arn" {
  description = "ARN of the SNS topic for schedule change notifications"
  value       = aws_sns_topic.schedule_changes.arn
}

output "sns_topic_name" {
  description = "Name of the SNS topic"
  value       = aws_sns_topic.schedule_changes.name
}

output "dynamodb_table_name" {
  description = "Name of the DynamoDB table for schedule storage"
  value       = aws_dynamodb_table.schedules.name
}

output "dynamodb_table_arn" {
  description = "ARN of the DynamoDB table"
  value       = aws_dynamodb_table.schedules.arn
}

output "eventbridge_rule_name" {
  description = "Name of the EventBridge rule (if enabled)"
  value       = var.enable_scheduled_checks ? aws_cloudwatch_event_rule.schedule_check[0].name : null
}
