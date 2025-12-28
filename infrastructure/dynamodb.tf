# DynamoDB Table for Schedule Storage
# Stores historical schedule data to detect changes

resource "aws_dynamodb_table" "schedules" {
  name           = "${var.schedule_table_name}-${var.environment}"
  billing_mode   = "PAY_PER_REQUEST"  # Cost-optimized: Only pay for what you use
  hash_key       = "team_id"
  range_key      = "game_id"
  
  # Hash key: team_id
  attribute {
    name = "team_id"
    type = "S"
  }
  
  # Range key: game_id (unique identifier for each game)
  attribute {
    name = "game_id"
    type = "S"
  }
  
  # Point-in-time recovery for data protection
  point_in_time_recovery {
    enabled = true
  }
  
  # Enable server-side encryption
  server_side_encryption {
    enabled     = true
    kms_key_arn = null  # Use AWS managed key for cost optimization
  }
  
  # TTL to automatically clean up old data (90 days)
  ttl {
    attribute_name = "ttl"
    enabled        = true
  }
  
  tags = {
    Name        = "${var.schedule_table_name}-${var.environment}"
    Description = "Storage for soccer game schedules"
  }
}
