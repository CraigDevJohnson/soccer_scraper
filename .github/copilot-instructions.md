## General Instructions

Always add verbose comments.
Use descriptive function and variable names.
Always add debugging when relevant.

## Architecture Overview

This is a Go application that fetches soccer schedules from the LPS (Let's Play Soccer) API and generates ICS calendar files. It runs as both an AWS Lambda function and a local CLI.

**Package structure:**

- `cmd/lambda/` - AWS Lambda entrypoint (API Gateway HTTP API v2)
- `cmd/scraper/` - Local CLI using urfave/cli
- `internal/app/` - Core handler logic, request routing, response formatting
- `internal/lps/` - LPS API client with concurrent fetching
- `internal/calendar/` - ICS generation with proper VTIMEZONE handling
- `internal/validate/` - Team ID validation (6-digit format)
- `internal/types/` - Shared types to avoid import cycles

**Key patterns:**

- Import cycle prevention: `internal/types/` contains shared structs (`Game`, `InvalidTeamID`, `FailedTeam`), with type aliases in `internal/app/types.go`
- Bounded concurrency: `errgroup.SetLimit(8)` in `lps/client.go` for parallel team fetches
- Embedded tzdata: `_ "time/tzdata"` import ensures `America/Denver` loads in Lambda
- Handler reuse: Lambda handler initialized in `init()` for connection pooling across invocations

## Build & Deployment

```bash
# Local CLI build
go build -o bin/scraper.exe ./cmd/scraper

# Lambda build (ARM64 for Graviton2)
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -tags lambda.norpc -o bootstrap ./cmd/lambda
```

Lambda requires:

- Runtime: `provided.al2023`
- Architecture: `arm64`
- Binary name: `bootstrap`

## API Contract

**Fetch (GET with query params):**

- `?action=fetch&team_ids=123456,654321`
- Returns JSON with `games`, `processed_team_ids`, `failed_teams`, `invalid_team_ids`

**Download (POST with JSON body):**

- `?action=download` + body: `{"games": [...]}`
- Returns `text/calendar` ICS file

## Testing

```bash
# Test CLI fetch
./bin/scraper.exe fetch -t 469306

# Test CLI download
./bin/scraper.exe download -t 469306 -o test.ics

# JSON output for debugging
./bin/scraper.exe fetch -t 469306 --json
```

## Library Workarounds

The `golang-ical` library (v0.3.2) lacks `AddDaylight()` on VTimezone. Work around by creating `&ics.Daylight{}` manually and appending to `tz.Components` (see `calendar/ics.go:addTimezone`).

## Timezone Handling

All times use `America/Denver` (Mountain Time). The ICS output includes a full VTIMEZONE with DST rules (MST/MDT transitions). API times come as UTC and are converted to MT for wall-clock display.
