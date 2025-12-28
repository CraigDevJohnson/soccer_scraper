# Soccer Scraper

A Go application that fetches soccer schedules from the LPS (Let's Play Soccer) API and generates ICS calendar files. Runs as both an AWS Lambda function and a local CLI.

## Architecture

**Package structure:**

- `cmd/lambda/` - AWS Lambda entrypoint (API Gateway HTTP API v2)
- `cmd/scraper/` - Local CLI using urfave/cli
- `internal/app/` - Core handler logic, request routing, response formatting
- `internal/lps/` - LPS API client with concurrent fetching
- `internal/calendar/` - ICS generation with proper VTIMEZONE handling
- `internal/validate/` - Team ID validation (6-digit format)
- `internal/types/` - Shared types to avoid import cycles

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
```

### AWS Lambda / API Gateway

**Fetch (GET with query params):**

- `?action=fetch&team_ids=123456,654321`
- Returns JSON with `games`, `processed_team_ids`, `failed_teams`, `invalid_team_ids`

**Download (POST with JSON body):**

- `?action=download` + body: `{"games": [...]}`
- Returns `text/calendar` ICS file

## Deployment

### AWS Lambda Deployment

1. Set up GitHub repository secrets:
   - `AWS_ACCESS_KEY_ID`
   - `AWS_SECRET_ACCESS_KEY`

2. Push to main branch to trigger automatic deployment, or manually trigger the workflow in GitHub Actions.

## Features

- Fetches soccer schedules from LPS API
- Concurrent team fetching with bounded concurrency (8 parallel requests)
- ICS calendar generation with proper VTIMEZONE (America/Denver) and DST handling
- Team ID validation (6-digit format)
- Dual deployment: AWS Lambda and local CLI

## Dependencies

- Go 1.21+
- See `go.mod` for module dependencies

## License

MIT License

## Contributing

Pull requests are welcome. For major changes, please open an issue first.
