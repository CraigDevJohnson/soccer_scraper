# Usage Guide: Schedule Change Notifications

This guide explains how to use the new schedule change notification features.

## Table of Contents
- [Overview](#overview)
- [Setup](#setup)
- [Subscribing to Notifications](#subscribing-to-notifications)
- [Unsubscribing](#unsubscribing)
- [How It Works](#how-it-works)
- [Troubleshooting](#troubleshooting)

## Overview

The Soccer Scraper now supports automatic email notifications when game schedules change. This includes:
- New games added to the schedule
- Existing games modified (time or field changes)
- Games cancelled or removed from the schedule

## Setup

### 1. Deploy Infrastructure

First, deploy the required AWS infrastructure (SNS, DynamoDB, EventBridge):

```bash
cd infrastructure
terraform init
terraform plan -var-file=terraform.tfvars
terraform apply -var-file=terraform.tfvars
```

See [infrastructure/README.md](infrastructure/README.md) for detailed deployment instructions.

### 2. Deploy Lambda Function

The Lambda function is automatically deployed via GitHub Actions when you push to the main branch. Alternatively, manually deploy using:

```bash
# Package the Lambda
pip install -r requirements-lambda.txt -t ./package
cp soccer_schedule_scraper.py ./package/
cd package
zip -r ../function.zip .

# Deploy to AWS
aws lambda update-function-code \
  --function-name soccer_schedule_scraper \
  --zip-file fileb://function.zip
```

### 3. Get Your Lambda URL

After deployment, get your Lambda function URL:

```bash
aws lambda get-function-url-config --function-name soccer_schedule_scraper
```

Or from Terraform outputs:

```bash
cd infrastructure
terraform output lambda_function_url
```

## Subscribing to Notifications

### Using curl

Subscribe your email address to receive notifications for specific teams:

```bash
curl "https://YOUR-LAMBDA-URL?action=subscribe&email=you@example.com&team_ids=123456,654321"
```

**Important**: You will receive a confirmation email from AWS SNS. You **must click the confirmation link** in that email to activate your subscription.

### Parameters

- `action=subscribe` - Required action parameter
- `email=YOUR_EMAIL` - Your email address
- `team_ids=ID1,ID2` - Comma-separated list of 6-digit team IDs to monitor

### Response

Success response:
```json
{
  "status": "subscribed",
  "email": "you@example.com",
  "subscription_arn": "arn:aws:sns:...",
  "message": "Subscription created. Please check your email to confirm."
}
```

If already subscribed:
```json
{
  "status": "already_subscribed",
  "email": "you@example.com",
  "subscription_arn": "arn:aws:sns:...",
  "message": "Email already subscribed. Updated monitored teams."
}
```

## Unsubscribing

To stop receiving notifications:

```bash
curl "https://YOUR-LAMBDA-URL?action=unsubscribe&email=you@example.com"
```

### Response

Success response:
```json
{
  "status": "unsubscribed",
  "email": "you@example.com",
  "message": "Successfully unsubscribed from notifications."
}
```

## How It Works

### Architecture

1. **Subscription Management**
   - When you subscribe, your email is added to an SNS topic
   - Team IDs you want to monitor are stored in DynamoDB

2. **Schedule Monitoring**
   - EventBridge triggers the Lambda function every hour (configurable)
   - Lambda fetches current schedules for all monitored teams
   - Current schedules are compared with stored schedules in DynamoDB

3. **Change Detection**
   - **New Games**: Games added to the schedule
   - **Modified Games**: Changes to date/time or field location
   - **Cancelled Games**: Games removed from the schedule

4. **Notification**
   - If changes are detected, an email is sent via SNS
   - Email includes details of all changes detected

### Email Notification Format

Example notification email:

```
Subject: Soccer Schedule Update - Team 123456

Schedule changes detected for team 123456:

🆕 NEW GAMES:
  • Fri 12/05 06:00 PM - Field 3: Team E vs Team F

📝 MODIFIED GAMES:
  Game: Team A vs Team B
    Date: Mon 12/01 06:00 PM → Mon 12/01 07:00 PM

❌ CANCELLED GAMES:
  • Wed 12/03 07:00 PM - Field 2: Team C vs Team D
```

### Schedule Check Frequency

By default, schedules are checked every 60 minutes. This can be configured in the infrastructure:

```hcl
# infrastructure/terraform.tfvars
schedule_check_frequency = 60  # minutes
```

## Troubleshooting

### Not Receiving Emails

1. **Check subscription confirmation**
   - Look for the confirmation email from AWS SNS
   - Check your spam folder
   - Click the confirmation link

2. **Verify subscription status**
   ```bash
   aws sns list-subscriptions-by-topic \
     --topic-arn "arn:aws:sns:REGION:ACCOUNT:soccer_schedule_changes-prod"
   ```

3. **Check Lambda logs**
   ```bash
   aws logs tail /aws/lambda/soccer_schedule_scraper --follow
   ```

### Invalid Team ID Error

Team IDs must be exactly 6 digits. Common issues:
- Too short/long: Must be exactly 6 digits (e.g., `123456` not `12345`)
- Non-numeric: Only numbers allowed
- Leading zeros: Are valid (e.g., `000123`)

### Schedule Not Updating

1. **Check EventBridge rule**
   ```bash
   aws events list-rules --name-prefix soccer_schedule_scraper
   ```

2. **Verify Lambda has permissions**
   ```bash
   aws lambda get-policy --function-name soccer_schedule_scraper
   ```

3. **Check DynamoDB table**
   ```bash
   aws dynamodb scan --table-name soccer_schedules-prod
   ```

### Lambda Execution Errors

View recent error logs:
```bash
aws logs filter-log-events \
  --log-group-name /aws/lambda/soccer_schedule_scraper \
  --filter-pattern "ERROR"
```

## Cost Estimates

Typical monthly costs (assuming moderate usage):

- **Lambda**: < $1 (1M requests free tier)
- **DynamoDB**: < $1 (25GB free tier)
- **SNS**: $0.50 per 1,000 email notifications
- **EventBridge**: < $0.10 (1M events free tier)
- **CloudWatch Logs**: < $1

**Total estimated cost: < $5/month**

## Additional Features

### Manual Schedule Check

Trigger a manual schedule check (useful for testing):

```bash
curl "https://YOUR-LAMBDA-URL?action=check_schedules"
```

### Fetch Current Schedule

Get the current schedule without subscribing:

```bash
curl "https://YOUR-LAMBDA-URL?action=fetch&team_ids=123456"
```

## Support

For issues or questions:
1. Check the [main README](README.md)
2. Check the [infrastructure README](infrastructure/README.md)
3. Open an issue on GitHub

## Security Notes

- Email addresses are stored in DynamoDB
- All data is encrypted at rest
- SNS topics use AWS managed encryption
- Lambda has minimal IAM permissions (least privilege)
