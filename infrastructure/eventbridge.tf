# EventBridge Rules for Scheduled Schedule Checks
# Feature flag controlled: can be enabled/disabled without affecting deployed resources

# EventBridge Rule for periodic schedule checking (if enabled)
resource "aws_cloudwatch_event_rule" "schedule_check" {
  count               = var.enable_scheduled_checks ? 1 : 0
  name                = "${var.lambda_function_name}-schedule-check-${var.environment}"
  description         = "Trigger Lambda to check for schedule changes"
  schedule_expression = "rate(${var.schedule_check_frequency} minutes)"
  
  tags = {
    Name        = "${var.lambda_function_name}-schedule-check"
    FeatureFlag = "scheduled_checks"
  }
}

# EventBridge Target for Lambda
resource "aws_cloudwatch_event_target" "lambda_target" {
  count = var.enable_scheduled_checks ? 1 : 0
  rule  = aws_cloudwatch_event_rule.schedule_check[0].name
  arn   = aws_lambda_function.scraper.arn
  
  input = jsonencode({
    action = "check_schedules"
  })
}

# Lambda Permission for EventBridge
resource "aws_lambda_permission" "allow_eventbridge" {
  count         = var.enable_scheduled_checks ? 1 : 0
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.scraper.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.schedule_check[0].arn
}
