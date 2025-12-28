# Lambda Function Configuration
# Note: Function code is deployed via GitHub Actions workflow

# Lambda Function (code deployed separately via CI/CD)
resource "aws_lambda_function" "scraper" {
  function_name = var.lambda_function_name
  role          = aws_iam_role.lambda_role.arn
  handler       = "soccer_schedule_scraper.lambda_handler"
  runtime       = "python3.9"
  timeout       = 60  # 60 seconds timeout
  memory_size   = 256 # 256 MB memory (cost-optimized)
  
  # Placeholder for initial deployment - actual code deployed via GitHub Actions
  filename      = data.archive_file.lambda_placeholder.output_path
  source_code_hash = data.archive_file.lambda_placeholder.output_base64sha256
  
  # Environment variables for Lambda
  environment {
    variables = {
      SNS_TOPIC_ARN    = aws_sns_topic.schedule_changes.arn
      DYNAMODB_TABLE   = aws_dynamodb_table.schedules.name
      ENVIRONMENT      = var.environment
    }
  }
  
  # Enable X-Ray tracing for debugging (minimal cost)
  tracing_config {
    mode = "PassThrough"
  }
  
  tags = {
    Name = var.lambda_function_name
  }
  
  # Prevent replacement on code updates (handled by CI/CD)
  lifecycle {
    ignore_changes = [
      filename,
      source_code_hash
    ]
  }
}

# Create a placeholder zip for initial deployment
data "archive_file" "lambda_placeholder" {
  type        = "zip"
  output_path = "${path.module}/lambda_placeholder.zip"
  
  source {
    content  = "# Placeholder - actual code deployed via GitHub Actions"
    filename = "placeholder.py"
  }
}

# CloudWatch Log Group for Lambda
resource "aws_cloudwatch_log_group" "lambda_logs" {
  name              = "/aws/lambda/${var.lambda_function_name}"
  retention_in_days = 7  # Cost-optimized: 7 days retention
  
  tags = {
    Name = "${var.lambda_function_name}-logs"
  }
}

# Lambda Function URL for HTTP access
resource "aws_lambda_function_url" "scraper_url" {
  function_name      = aws_lambda_function.scraper.function_name
  authorization_type = "NONE"  # Public access
  
  cors {
    allow_origins     = ["*"]
    allow_methods     = ["GET", "POST"]
    allow_headers     = ["*"]
    expose_headers    = ["*"]
    max_age          = 3600
  }
}
