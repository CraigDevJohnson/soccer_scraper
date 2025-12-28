# IAM Roles and Policies for Lambda Function
# Provides least-privilege access to required AWS services

# IAM Role for Lambda Function
resource "aws_iam_role" "lambda_role" {
  name               = "${var.lambda_function_name}-role-${var.environment}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
  
  tags = {
    Name = "${var.lambda_function_name}-role-${var.environment}"
  }
}

# CloudWatch Logs Policy for Lambda
resource "aws_iam_role_policy" "lambda_logs_policy" {
  name = "${var.lambda_function_name}-logs-policy-${var.environment}"
  role = aws_iam_role.lambda_role.id
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.lambda_function_name}:*"
      }
    ]
  })
}

# DynamoDB Access Policy for Lambda
resource "aws_iam_role_policy" "lambda_dynamodb_policy" {
  name = "${var.lambda_function_name}-dynamodb-policy-${var.environment}"
  role = aws_iam_role.lambda_role.id
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "dynamodb:PutItem",
          "dynamodb:GetItem",
          "dynamodb:Query",
          "dynamodb:Scan",
          "dynamodb:UpdateItem",
          "dynamodb:DeleteItem",
          "dynamodb:BatchWriteItem"
        ]
        Resource = [
          aws_dynamodb_table.schedules.arn,
          "${aws_dynamodb_table.schedules.arn}/index/*"
        ]
      }
    ]
  })
}

# SNS Access Policy for Lambda
resource "aws_iam_role_policy" "lambda_sns_policy" {
  name = "${var.lambda_function_name}-sns-policy-${var.environment}"
  role = aws_iam_role.lambda_role.id
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "sns:Publish",
          "sns:Subscribe",
          "sns:Unsubscribe",
          "sns:ListSubscriptionsByTopic",
          "sns:GetTopicAttributes"
        ]
        Resource = aws_sns_topic.schedule_changes.arn
      }
    ]
  })
}
