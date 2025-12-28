# Soccer Scraper Infrastructure

This directory contains OpenTofu/Terraform configuration for deploying the Soccer Scraper infrastructure on AWS.

## Architecture

The infrastructure consists of:

- **Lambda Function**: Processes soccer schedules and handles notifications
- **DynamoDB Table**: Stores historical schedule data for change detection
- **SNS Topic**: Manages email subscriptions and sends notifications
- **EventBridge Rule**: Periodically checks for schedule changes (optional)
- **IAM Roles & Policies**: Least-privilege access for Lambda

## Prerequisites

- OpenTofu >= 1.6.0 or Terraform >= 1.6.0
- AWS CLI configured with appropriate credentials
- AWS account with necessary permissions

## Cost Optimization

This infrastructure is designed with cost optimization as the top priority:

- **DynamoDB**: Uses PAY_PER_REQUEST billing (only pay for what you use)
- **Lambda**: 256 MB memory, efficient for most workloads
- **CloudWatch Logs**: 7-day retention to minimize storage costs
- **SNS**: Only pay for notifications sent
- **EventBridge**: Minimal cost for periodic triggers

Estimated monthly cost: < $5 for moderate usage

## Deployment

### 1. Configure Backend (Optional but Recommended)

Create a backend configuration file `backend.tfvars`:

```hcl
bucket = "your-terraform-state-bucket"
key    = "soccer-scraper/terraform.tfstate"
region = "us-west-2"
```

### 2. Initialize OpenTofu

```bash
cd infrastructure
tofu init -backend-config=backend.tfvars
```

Or for local state:

```bash
tofu init -backend=false
```

### 3. Create Variables File

Copy the example and customize:

```bash
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with your values
```

### 4. Plan and Apply

```bash
# Review changes
tofu plan -var-file=terraform.tfvars

# Apply changes
tofu apply -var-file=terraform.tfvars
```

## Feature Flags

### Scheduled Checks

The scheduled checks feature can be enabled/disabled via the `enable_scheduled_checks` variable:

```hcl
enable_scheduled_checks = true  # Enable automatic schedule checking
```

When disabled, the EventBridge rule and associated resources are not created. This allows you to:
- Test the infrastructure without automated checks
- Reduce costs during development
- Manually trigger checks only when needed

**Warning**: Disabling this feature after it's been enabled will delete the EventBridge rule. Existing subscriptions and stored schedules will not be affected.

## Variables

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `aws_region` | AWS region for deployment | `us-west-2` | No |
| `environment` | Environment name (dev/staging/prod) | `prod` | No |
| `lambda_function_name` | Lambda function name | `soccer_schedule_scraper` | No |
| `schedule_table_name` | DynamoDB table name | `soccer_schedules` | No |
| `sns_topic_name` | SNS topic name | `soccer_schedule_changes` | No |
| `schedule_check_frequency` | Check frequency in minutes | `60` | No |
| `enable_scheduled_checks` | Enable automated checking | `true` | No |

## Outputs

After deployment, the following outputs are available:

- `lambda_function_url`: URL for accessing the Lambda function
- `sns_topic_arn`: ARN of the SNS topic
- `dynamodb_table_name`: Name of the DynamoDB table

## Usage After Deployment

### Subscribe to Notifications

```bash
curl -X POST "https://your-lambda-url?action=subscribe&team_ids=123456&email=user@example.com"
```

### Unsubscribe

```bash
curl -X POST "https://your-lambda-url?action=unsubscribe&email=user@example.com"
```

## Security Best Practices

- SNS topic uses AWS managed encryption
- DynamoDB table uses server-side encryption
- IAM roles follow least-privilege principle
- Lambda function logs are retained for 7 days only
- Point-in-time recovery enabled on DynamoDB

**Important Security Note**: The Lambda function URL is publicly accessible without authentication. This is intentional for ease of use but means:
- Anyone with the URL can subscribe/unsubscribe emails (requires email confirmation via SNS)
- Consider implementing API Gateway with authentication for production use
- Monitor CloudWatch Logs for suspicious activity
- Consider implementing rate limiting via AWS WAF if abuse occurs

For production environments, we recommend:
1. Using API Gateway with API keys or JWT authentication
2. Implementing rate limiting with AWS WAF
3. Adding request validation and sanitization
4. Monitoring subscription patterns for abuse
- DynamoDB table uses server-side encryption
- IAM roles follow least-privilege principle
- Lambda function logs are retained for 7 days only
- Point-in-time recovery enabled on DynamoDB

## Maintenance

### Updating Infrastructure

```bash
# Pull latest changes
git pull

# Review and apply updates
tofu plan -var-file=terraform.tfvars
tofu apply -var-file=terraform.tfvars
```

### Destroying Infrastructure

```bash
# WARNING: This will delete all resources and data
tofu destroy -var-file=terraform.tfvars
```

## Troubleshooting

### Lambda Not Receiving Events

Check EventBridge rule is enabled and Lambda has permission:

```bash
aws events list-rules --name-prefix soccer_schedule_scraper
aws lambda get-policy --function-name soccer_schedule_scraper
```

### SNS Subscription Issues

Verify email subscriptions are confirmed:

```bash
aws sns list-subscriptions-by-topic --topic-arn <sns-topic-arn>
```

### DynamoDB Access Issues

Check Lambda role has necessary permissions:

```bash
aws iam get-role-policy --role-name soccer_schedule_scraper-role-prod --policy-name soccer_schedule_scraper-dynamodb-policy-prod
```
