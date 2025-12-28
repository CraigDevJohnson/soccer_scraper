# Soccer Scraper

A Python script for scraping soccer-related data, deployable as AWS Lambda function with schedule change notifications via SNS.

## Description

This project contains scripts to scrape and process soccer/football data from various sources. The data can be used for analysis, statistics, or other soccer-related applications. It now includes automatic notifications when game schedules change.

## Features

- Data scraping from soccer websites
- Data processing and cleaning
- Calendar export functionality (.ics format)
- AWS Lambda support with GitHub Actions deployment
- **Email notifications for schedule changes via SNS**
- **Automatic schedule monitoring with DynamoDB storage**
- **Subscribe/Unsubscribe functionality for email notifications**

## Local Installation

```bash
pip install -r requirements-lambda.txt
```

## Infrastructure Deployment

The project includes OpenTofu/Terraform infrastructure for deploying to AWS. See the [infrastructure README](./infrastructure/README.md) for details.

Quick start:
```bash
cd infrastructure
tofu init
tofu plan -var-file=terraform.tfvars
tofu apply -var-file=terraform.tfvars
```

## Usage

### Local Usage

```python
python soccer_schedule_scraper.py
```

### AWS Lambda Usage

#### Fetch Schedule

Call the function with:

```bash
curl "https://your-lambda-url?action=fetch&team_ids=123456,654321"
```

Or via JSON:

```json
{
    "action": "fetch",
    "team_ids": ["123456", "654321"]
}
```

#### Subscribe to Schedule Change Notifications

Subscribe an email address to receive notifications when game schedules change:

```bash
curl "https://your-lambda-url?action=subscribe&email=user@example.com&team_ids=123456,654321"
```

**Note**: You will receive a confirmation email from AWS SNS. You must click the confirmation link to start receiving notifications.

#### Unsubscribe from Notifications

```bash
curl "https://your-lambda-url?action=unsubscribe&email=user@example.com"
```

#### Download Calendar

```bash
curl -X POST "https://your-lambda-url?action=download" \
  -H "Content-Type: application/json" \
  -d '{"games": [...]}'
```

## Deployment

### AWS Lambda Deployment

1. Set up GitHub repository secrets:
   - AWS_ACCESS_KEY_ID
   - AWS_SECRET_ACCESS_KEY

2. Deploy infrastructure first (see infrastructure/README.md)

3. Push to main branch to trigger automatic deployment, or manually trigger the workflow in GitHub Actions.

## How Schedule Change Notifications Work

1. **Subscribe**: Users subscribe to notifications for specific team IDs
2. **Storage**: Current schedules are stored in DynamoDB
3. **Monitoring**: EventBridge triggers Lambda every hour (configurable) to check for changes
4. **Comparison**: Lambda compares current schedule with stored schedule
5. **Notification**: If changes detected (new games, modified times/fields, cancelled games), SNS sends email to all subscribers

### What Changes are Detected?

- **New Games**: Games added to the schedule
- **Modified Games**: Changes to game date/time or field location
- **Cancelled Games**: Games removed from the schedule

## Dependencies

- Python 3.9+
- Required packages listed in requirements-lambda.txt
- AWS Services: Lambda, DynamoDB, SNS, EventBridge (for notifications)

## Cost Optimization

The infrastructure is designed to minimize AWS costs:
- DynamoDB uses on-demand pricing
- Lambda runs only when needed
- CloudWatch logs retained for 7 days only
- SNS charges only for emails sent

Estimated cost: < $5/month for moderate usage

## License

MIT License

## Contributing

Pull requests are welcome. For major changes, please open an issue first.