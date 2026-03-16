# Feature: Migrate Local CLI from urfave/cli/v2 to urfave/cli/v3

## Overview

Refactor the local CLI in `cmd/scraper` from `github.com/urfave/cli/v2`
to `github.com/urfave/cli/v3` while preserving the documented user
experience for `fetch`, `download`, `subscribe`, and `check-changes`.
This migration is needed because urfave/cli v3 removes `cli.Context`,
changes handler signatures to accept `context.Context` and `*cli.Command`,
and moves the root app model toward running a command directly.

The AWS Lambda binary in `cmd/lambda` does not import urfave/cli, so the
migration scope is limited to the local CLI and repo assets that describe or
verify CLI behavior. Even so, the repository must continue to build and test
cleanly because the same project is deployed as an AWS Lambda function.

## Success Criteria

- [x] All tasks complete
- [x] All tests passing
- [x] Build succeeds
- [x] No blockers

## Tasks

### Task-001: Baseline Current CLI Behavior and Migration Surface

**Priority**: High
**Estimated Iterations**: 1-2

**Acceptance Criteria**:

- [x] Inventory all `urfave/cli` usage in the repository
- [x] Confirm the migration target is limited to `cmd/scraper` and module metadata
- [x] Capture the current CLI contract from the README and code for these commands: `fetch`, `download`, `subscribe`, `check-changes`
- [x] Document v2 to v3 breaking changes relevant to this codebase: import path, root command model, removal of `cli.Context`, and handler signature changes
- [x] Identify any repo assets that need follow-up updates, including README usage examples and dependency references

**Verification**:

```bash
# Confirm current CLI dependency locations before migration work starts
rg -n "urfave/cli|github.com/urfave/cli/v2|github.com/urfave/cli/v3" .
```

**Verified Baseline Notes (2026-03-13)**:

- `urfave/cli` runtime usage is isolated to `cmd/scraper/main.go`, which imports `github.com/urfave/cli/v2`, builds a `cli.App`, declares four `*cli.Command` entries, and uses `*cli.Context` in `prepareClientAndTeams`, `fetchAction`, `downloadAction`, `subscribeAction`, and `checkChangesAction`.
- Dependency metadata currently references v2 in `go.mod` and `go.sum`.
- Documentation and repo guidance that reference the current CLI framework live in `README.md` and `.github/copilot-instructions.md`.
- No `urfave/cli` usage was found under `cmd/lambda`, so the implementation migration surface is limited to `cmd/scraper` plus module metadata and follow-up documentation references.
- Current CLI contract from code and README:
  - `fetch` (`f`): requires `--team-ids` / `-t`; optional `--json` / `-j`; prints invalid team IDs before continuing with valid IDs; returns an error if no valid team IDs remain; outputs either formatted schedules or JSON.
  - `download` (`d`): requires `--team-ids` / `-t`; optional `--output` / `-o`; fetches schedules, generates ICS content, and writes either the provided filename or an auto-generated filename.
  - `subscribe` (`s`): requires `--team-id` / `-t` and `--email` / `-e`; validates the team ID and email address, fetches the current schedule, creates or reuses the SNS topic, subscribes the email address, and stores the schedule in DynamoDB for change tracking.
  - `check-changes` (`c`): optional `--team-id` / `-t`; when set, validates and checks one team; otherwise checks all subscribed teams and reports the totals.
- Relevant urfave/cli v2 to v3 breaking changes for this repo, verified against the official migration guide and v3 getting started docs:
  - Import path changes from `github.com/urfave/cli/v2` to `github.com/urfave/cli/v3`.
  - The root entrypoint moves away from constructing and running `cli.App` and toward running a root `cli.Command` directly with `Run(context.Background(), os.Args)`.
  - `cli.Context` is removed; flag and argument access that currently flows through `*cli.Context` must move onto `*cli.Command` APIs.
  - Action and related handler signatures change from `func(*cli.Context) error` to handlers that receive `context.Context` and `*cli.Command`.
- Follow-up assets for later tasks:
  - `README.md` command examples and architecture text that mention urfave/cli.
  - `README.md` usage wording for the first `fetch` example, which currently says JSON while the code only emits JSON when `--json` is set.
  - `.github/copilot-instructions.md` references to urfave/cli and the pinned v2 dependency.
  - `go.mod` and `go.sum` dependency references.

### Task-002: Refactor the CLI Entrypoint to the v3 Command Model

**Priority**: High
**Estimated Iterations**: 2-3

**Acceptance Criteria**:

- [x] Replace `github.com/urfave/cli/v2` with `github.com/urfave/cli/v3` in the module and CLI entrypoint
- [x] Convert the root CLI construction from `cli.App` usage to the v3 command-based entrypoint pattern
- [x] Update all command actions and helpers to use v3 handler signatures with `context.Context` and `*cli.Command`
- [x] Remove all direct dependence on `*cli.Context` and replace flag access with the appropriate v3 command APIs
- [x] Preserve command names, aliases, required flags, and output behavior unless v3 requires a deliberate documented deviation
- [x] Keep the implementation localized to the CLI path without changing Lambda request handling behavior

**Verification**:

```bash
# Ensure the CLI package compiles after the API migration
go build -o bin/scraper ./cmd/scraper
```

### Task-003: Resolve Dependency and Compile-Time Fallout

**Priority**: High
**Estimated Iterations**: 1-2

**Acceptance Criteria**:

- [x] Update `go.mod` and `go.sum` to the v3 dependency set
- [x] Resolve any compiler errors introduced by changed urfave/cli types or signatures
- [x] Confirm no stale v2 imports remain anywhere in the repository
- [x] Keep unrelated dependency changes out of scope unless required for the CLI migration to build
- [x] Format the Go code and keep the resulting diff minimal

**Verification**:

```bash
# Check for stale imports and verify the codebase still compiles
rg -n "github.com/urfave/cli/v2" .
go test ./...
```

### Task-004: Revalidate CLI Behavior with Focused Smoke Tests

**Priority**: High
**Estimated Iterations**: 2-3

**Acceptance Criteria**:

- [x] Verify help output still exposes the expected commands and flags
- [x] Verify `fetch -t 469306 --json` still executes and returns JSON output
- [x] Verify `download` still produces an ICS file; if team `469306` has no upcoming live games, rerun the same smoke check with another currently active team ID and record the live-data variance
- [x] Verify validation and error paths still behave correctly for invalid team IDs and invalid emails
- [x] Verify `check-changes` and `check-changes -t 469306` still parse options correctly, with AWS-dependent execution called out when credentials are required
- [x] Record any intentional user-visible changes introduced by v3 semantics

**Verification**:

```bash
# Smoke-check the migrated CLI
go build -o bin/scraper ./cmd/scraper
./bin/scraper --help
./bin/scraper fetch -t 469306 --json
./bin/scraper download -t 469306 -o /tmp/soccer_schedule.ics
# If team 469306 has no upcoming live games, rerun download with another active team ID and record the substitution.
./bin/scraper fetch -t invalid
```

### Task-005: Update Documentation and Migration Notes

**Priority**: Medium
**Estimated Iterations**: 1-2

**Acceptance Criteria**:

- [x] Update README content that references the CLI framework or build and usage expectations if anything user-visible changes
- [x] Update repository guidance that still mentions `urfave/cli/v2`
- [x] Keep deployment notes clear that the Lambda binary is unaffected by the CLI framework migration
- [x] Document the final verification commands used during migration

**Verification**:

- Manual test: Review README command examples against actual CLI behavior after migration
- Automated: `go test ./...`

## Technical Constraints

- Language: Go 1.24.0
- Framework: urfave/cli v3 for the local CLI only
- Testing: `go test ./...` plus manual CLI smoke checks
- Style: `go fmt ./...` and existing repository conventions

## Architecture Notes

- Design pattern: Keep the current split between the local CLI in `cmd/scraper` and the Lambda runtime in `cmd/lambda`
- Key libraries: `github.com/urfave/cli/v3`, `github.com/aws/aws-lambda-go`, `github.com/aws/aws-sdk-go-v2`, `github.com/arran4/golang-ical`
- Data flow: CLI commands parse flags, invoke existing internal packages (`lps`, `calendar`, `notify`, `sns`, `storage`, `validate`), and print or persist results; Lambda continues to route API Gateway and EventBridge events without depending on urfave/cli
- Migration notes from urfave docs: v3 requires the new import path, removes `cli.Context`, changes action signatures to receive `context.Context` and `*cli.Command`, and encourages running the root command directly with `Run(context.Background(), os.Args)`

## Out of Scope

- Refactoring the AWS Lambda handler architecture
- Adding new CLI features or changing command semantics beyond what is required for v3 compatibility
- Reworking business logic in `internal/*` packages except where compilation forces a narrow interface adjustment
- Infrastructure, deployment workflow, or scheduler changes unrelated to the CLI migration
