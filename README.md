# Soccer Scraper

A Go application that fetches soccer schedules from the LPS (Let's Play Soccer) API and generates ICS calendar files. Runs as both an AWS Lambda function and a local CLI. Now includes email notification support for schedule changes.

## Architecture

**Package structure:**

- `cmd/lambda/` - AWS Lambda entrypoint (API Gateway HTTP API v2 + EventBridge scheduled events)
- `cmd/scraper/` - Local CLI using urfave/cli
- `internal/app/` - Core handler logic, request routing, response formatting
- `internal/lps/` - LPS API client with concurrent fetching
- `internal/calendar/` - ICS generation with proper VTIMEZONE handling
- `internal/validate/` - Team ID validation (6-digit format)
- `internal/types/` - Shared types to avoid import cycles
- `internal/sns/` - AWS SNS topic management for email notifications
- `internal/storage/` - DynamoDB schedule persistence with TTL
- `internal/notify/` - Schedule comparison and change detection

## Build

### Local CLI Build

```bash
go build -o bin/scraper.exe ./cmd/scraper
```

### AWS Lambda Build (ARM64 for Graviton2)

```bash
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -tags lambda.norpc -o bootstrap ./cmd/lambda
```

Lambda requirements:

- Runtime: `provided.al2023`
- Architecture: `arm64`
- Binary name: `bootstrap`

## Usage

### CLI Usage

```bash
# Fetch games as JSON
./bin/scraper.exe fetch -t 469306

# Fetch with JSON output for debugging
./bin/scraper.exe fetch -t 469306 --json

# Download and save ICS file
./bin/scraper.exe download -t 469306 -o schedule.ics

# Subscribe to email notifications for schedule changes
./bin/scraper.exe subscribe -t 469306 -e your-email@example.com

# Check for schedule changes (runs once, for scheduled jobs)
./bin/scraper.exe check-changes

# Check a specific team for changes
./bin/scraper.exe check-changes -t 469306
```

### Email Notifications

The subscribe command sets up email notifications for schedule changes:

1. **Subscribe**: Run `subscribe -t TEAMID -e EMAIL` to register for notifications
2. **Confirm**: Check your email and click the confirmation link from AWS SNS
3. **Receive Updates**: Get notified when games are added, removed, or rescheduled

**What triggers notifications:**
- New games added to the schedule
- Games cancelled or removed
- Game time changes
- Field changes

**Schedule storage:**
- Schedules are stored in DynamoDB with a 90-day TTL (covers 8-week season + buffer)
- The `check-changes` command compares current API data with stored schedules
- Run `check-changes` on a schedule (e.g., daily via cron or CloudWatch Events)

### AWS Lambda / API Gateway

**Fetch (GET with query params):**

- `?action=fetch&team_ids=123456,654321`
- Returns JSON with `games`, `processed_team_ids`, `failed_teams`, `invalid_team_ids`

**Download (POST with JSON body):**

- `?action=download` + body: `{"games": [...]}`
- Returns `text/calendar` ICS file

**Subscribe (GET with query params):**

- `?action=subscribe&team_id=123456&email=user@example.com`
- Returns JSON with subscription details: `{"success": true, "message": "...", "team_id": "...", "team_name": "...", "email": "...", "subscription_arn": "...", "topic_arn": "..."}`
- Subscribes the email address to schedule change notifications for the specified team
- User must confirm the subscription via the email sent by AWS SNS

## Deployment

### AWS Lambda Deployment

1. Set up GitHub repository secrets:
   - `AWS_ACCESS_KEY_ID`
   - `AWS_SECRET_ACCESS_KEY`

2. Push to main branch to trigger automatic deployment, or manually trigger the workflow in GitHub Actions.

### AWS Infrastructure for Email Notifications

The email notification feature requires the following AWS resources:

1. **DynamoDB Table**: `soccer-schedules`
   - Partition key: `team_id` (String)
   - TTL attribute: `ttl` (enabled)

2. **SNS Topics**: Created automatically with prefix `soccer-schedule-`
   - Topics are created per team when users subscribe

3. **IAM Permissions** (for Lambda or CLI):
   ```json
   {
     "Effect": "Allow",
     "Action": [
       "dynamodb:PutItem",
       "dynamodb:GetItem",
       "dynamodb:DeleteItem",
       "dynamodb:Scan"
     ],
     "Resource": "arn:aws:dynamodb:*:*:table/soccer-schedules"
   },
   {
     "Effect": "Allow",
     "Action": [
       "sns:CreateTopic",
       "sns:Subscribe",
       "sns:Publish"
     ],
     "Resource": "arn:aws:sns:*:*:soccer-schedule-*"
   }
   ```

4. **EventBridge Scheduler** (for automatic schedule checking with timezone support):
   
   The Lambda automatically detects EventBridge scheduled events and runs the `check-changes` logic. Use **AWS EventBridge Scheduler** (not EventBridge Rules) to run at exactly 3 AM and 3 PM Mountain Time year-round, automatically adjusting for Daylight Saving Time.

   **Using AWS EventBridge Scheduler (Recommended):**
   
   EventBridge Scheduler supports timezone-aware scheduling, so the schedule will always run at the correct local time regardless of DST changes.

   ```bash
   # Create morning schedule (3 AM Mountain Time, every day)
   aws scheduler create-schedule \
     --name soccer-schedule-check-morning \
     --schedule-expression "cron(0 3 * * ? *)" \
     --schedule-expression-timezone "America/Denver" \
     --flexible-time-window '{"Mode":"OFF"}' \
     --target '{
       "Arn": "arn:aws:lambda:us-west-2:ACCOUNT_ID:function:soccer_schedule_scraper",
       "RoleArn": "arn:aws:iam::ACCOUNT_ID:role/EventBridgeSchedulerRole"
     }' \
     --state ENABLED

   # Create afternoon schedule (3 PM Mountain Time, every day)
   aws scheduler create-schedule \
     --name soccer-schedule-check-afternoon \
     --schedule-expression "cron(0 15 * * ? *)" \
     --schedule-expression-timezone "America/Denver" \
     --flexible-time-window '{"Mode":"OFF"}' \
     --target '{
       "Arn": "arn:aws:lambda:us-west-2:ACCOUNT_ID:function:soccer_schedule_scraper",
       "RoleArn": "arn:aws:iam::ACCOUNT_ID:role/EventBridgeSchedulerRole"
     }' \
     --state ENABLED
   ```

   **Create the IAM Role for Scheduler:**
   ```bash
   # Create the role
   aws iam create-role \
     --role-name EventBridgeSchedulerRole \
     --assume-role-policy-document '{
       "Version": "2012-10-17",
       "Statement": [{
         "Effect": "Allow",
         "Principal": {"Service": "scheduler.amazonaws.com"},
         "Action": "sts:AssumeRole"
       }]
     }'

   # Attach Lambda invoke permission
   aws iam put-role-policy \
     --role-name EventBridgeSchedulerRole \
     --policy-name InvokeLambda \
     --policy-document '{
       "Version": "2012-10-17",
       "Statement": [{
         "Effect": "Allow",
         "Action": "lambda:InvokeFunction",
         "Resource": "arn:aws:lambda:us-west-2:ACCOUNT_ID:function:soccer_schedule_scraper"
       }]
     }'
   ```

   **Cost Optimization:**
   - The Lambda checks if there are any teams in DynamoDB before processing
   - If no teams are subscribed, it returns immediately without making API calls
   - This minimizes costs when no one is using the notification feature

## Features

- Fetches soccer schedules from LPS API
- Concurrent team fetching with bounded concurrency (8 parallel requests)
- ICS calendar generation with proper VTIMEZONE (America/Denver) and DST handling
- Team ID validation (6-digit format)
- Dual deployment: AWS Lambda and local CLI
- **Email notifications for schedule changes:**
  - Subscribe to teams via SNS topics
  - Automatic change detection comparing stored vs current schedules
  - Notifications for added, removed, and updated games
  - 90-day TTL for schedule data (covers 8-week seasons)
  - **EventBridge scheduled rules** check twice daily (3 AM and 3 PM Mountain Time)
  - **Cost optimized** - skips processing when no teams are subscribed

## Dependencies

- Go 1.21+
- See `go.mod` for module dependencies

## License

MIT License

## Contributing

Pull requests are welcome. For major changes, please open an issue first.
