# SNS Topic for Schedule Change Notifications
# This topic allows users to subscribe for email notifications when game schedules change

# SNS Topic for schedule change notifications
resource "aws_sns_topic" "schedule_changes" {
  name              = "${var.sns_topic_name}-${var.environment}"
  display_name      = "Soccer Schedule Changes"
  
  # Enable encryption at rest
  kms_master_key_id = "alias/aws/sns"
  
  tags = {
    Name        = "${var.sns_topic_name}-${var.environment}"
    Description = "Notifications for soccer schedule changes"
  }
}

# SNS Topic Policy to allow Lambda to publish
resource "aws_sns_topic_policy" "schedule_changes_policy" {
  arn = aws_sns_topic.schedule_changes.arn
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowLambdaPublish"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
        Action = [
          "SNS:Publish"
        ]
        Resource = aws_sns_topic.schedule_changes.arn
        Condition = {
          StringEquals = {
            "aws:SourceAccount" = data.aws_caller_identity.current.account_id
          }
        }
      },
      {
        Sid    = "AllowSubscribe"
        Effect = "Allow"
        Principal = {
          AWS = aws_iam_role.lambda_role.arn
        }
        Action = [
          "SNS:Subscribe",
          "SNS:Unsubscribe",
          "SNS:ListSubscriptionsByTopic"
        ]
        Resource = aws_sns_topic.schedule_changes.arn
      }
    ]
  })
}

# Data source to get current AWS account ID
data "aws_caller_identity" "current" {}
