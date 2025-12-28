# Quick Reference: Schedule Notifications

## Subscribe to Notifications
```bash
curl "https://YOUR-LAMBDA-URL?action=subscribe&email=you@example.com&team_ids=123456"
```
**Don't forget**: Click the confirmation link in your email!

## Unsubscribe
```bash
curl "https://YOUR-LAMBDA-URL?action=unsubscribe&email=you@example.com"
```

## Fetch Schedule (No Subscription)
```bash
curl "https://YOUR-LAMBDA-URL?action=fetch&team_ids=123456"
```

## Manual Check for Changes
```bash
curl "https://YOUR-LAMBDA-URL?action=check_schedules"
```

## Infrastructure Deployment

### First Time Setup
```bash
cd infrastructure
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with your settings
terraform init
terraform plan
terraform apply
```

### Get Lambda URL
```bash
terraform output lambda_function_url
```

## What Gets Notified?

- 🆕 **New games** added to schedule
- 📝 **Modified games** (time or field changes)
- ❌ **Cancelled games** removed from schedule

## Notification Frequency

Default: Every 60 minutes
Configure in `infrastructure/terraform.tfvars`:
```hcl
schedule_check_frequency = 60  # minutes
```

## Troubleshooting

### Check Lambda Logs
```bash
aws logs tail /aws/lambda/soccer_schedule_scraper --follow
```

### Verify Subscription
```bash
aws sns list-subscriptions-by-topic --topic-arn YOUR-TOPIC-ARN
```

### Check DynamoDB Data
```bash
aws dynamodb scan --table-name soccer_schedules-prod
```

## Cost Estimate
< $5/month for moderate usage

## Need Help?
- See [USAGE.md](USAGE.md) for detailed guide
- See [infrastructure/README.md](infrastructure/README.md) for deployment details
- See [README.md](README.md) for general information
