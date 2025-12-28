## General Instructions

Always add verbose comments.
Use descriptive function and variable names.
Always add debugging when relevant.

## Go Coding Conventions

**Documentation:**
- Every package must have a package comment describing its purpose (see existing examples)
- All exported functions, types, and constants must have doc comments
- Doc comments should be complete sentences starting with the name being documented
- Use descriptive parameter and return value documentation for complex functions

**Code Style:**
- Use `go fmt` to format code (enforced automatically)
- Follow standard Go naming conventions (camelCase for unexported, PascalCase for exported)
- Keep functions focused and single-purpose
- Use meaningful variable names (avoid single-letter names except for common idioms like `i` in loops)
- Prefer explicit error handling over panic/recover

**Concurrency:**
- Use `errgroup.SetLimit()` for bounded concurrency (see `lps/client.go` for pattern)
- Always handle context cancellation properly
- Document goroutine lifecycles and synchronization mechanisms

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

**Unit Tests:**
- Currently no unit tests exist (all packages show `[no test files]`)
- When adding tests, follow Go testing conventions:
  - Place test files next to the code they test with `_test.go` suffix
  - Use table-driven tests for multiple test cases
  - Use subtests with `t.Run()` for better organization
  - Mock external dependencies (HTTP clients, API calls)
- Run tests with: `go test ./...`
- Run tests with coverage: `go test -cover ./...`

**Manual Testing:**
```bash
# Build the CLI first
go build -o bin/scraper.exe ./cmd/scraper

# Test CLI fetch
./bin/scraper.exe fetch -t 469306

# Test CLI download
./bin/scraper.exe download -t 469306 -o test.ics

# JSON output for debugging
./bin/scraper.exe fetch -t 469306 --json

# Test with multiple teams
./bin/scraper.exe fetch -t 469306,123456

# Test invalid team ID handling
./bin/scraper.exe fetch -t invalid
```

**Lambda Testing:**
- Lambda function can be tested locally with AWS SAM or similar tools
- Deploy to AWS and test with API Gateway endpoints

## Library Workarounds

The `golang-ical` library (v0.3.2) lacks `AddDaylight()` on VTimezone. Work around by creating `&ics.Daylight{}` manually and appending to `tz.Components` (see `calendar/ics.go:addTimezone`).

## Timezone Handling

All times use `America/Denver` (Mountain Time). The ICS output includes a full VTIMEZONE with DST rules (MST/MDT transitions). API times come as UTC and are converted to MT for wall-clock display.

## Development Workflow

**Code Quality:**
- Format code with `go fmt ./...` before committing
- Run `go vet ./...` to catch common mistakes
- No linter configuration exists currently; standard Go best practices apply

**Dependencies:**
- Manage dependencies with Go modules (`go.mod` and `go.sum`)
- Add dependencies: `go get <package>`
- Update dependencies: `go get -u <package>`
- Tidy dependencies: `go mod tidy`
- Vendor dependencies are not used; rely on Go module cache

**Version Requirements:**
- Go 1.24.0 as specified in `go.mod`
- Key dependencies:
  - `github.com/arran4/golang-ical` v0.3.2 - ICS calendar generation
  - `github.com/aws/aws-lambda-go` v1.51.1 - Lambda runtime
  - `github.com/urfave/cli/v2` v2.27.7 - CLI framework
  - `golang.org/x/sync` v0.19.0 - Concurrency primitives (errgroup)

## Common Tasks

**Add new validation logic:**
- Add to `internal/validate/` package
- Follow existing pattern with exported functions and clear error messages
- Update `ParseTeamIDsCSV` if validation affects multiple IDs

**Add new API endpoints or actions:**
- Update `internal/app/handler.go` router
- Add response types to `internal/app/types.go`
- Update README.md API contract section

**Modify game data structure:**
- Update `internal/types/types.go` first (shared types)
- Use type aliases in consumer packages to avoid import cycles
- Update both fetch and download handlers to handle new fields

**Change timezone or calendar format:**
- Modify `internal/calendar/ics.go`
- Test thoroughly as ICS format is strict and timezone handling is complex
- Remember the golang-ical library limitations (manual VTIMEZONE components)
