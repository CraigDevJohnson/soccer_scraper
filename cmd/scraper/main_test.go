package main

import (
	"context"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/CraigDevJohnson/soccer_scraper/internal/app"
	"github.com/CraigDevJohnson/soccer_scraper/internal/lps"
	"github.com/urfave/cli/v3"
)

// captureStdout redirects stdout while the provided callback runs.
// The helper keeps the tests focused on behavior assertions instead of
// repeatedly duplicating pipe setup and teardown logic.
func captureStdout(t *testing.T, callback func()) string {
	t.Helper()

	originalStdout := os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe() error = %v", err)
	}

	os.Stdout = writer

	callback()

	if err := writer.Close(); err != nil {
		t.Fatalf("writer.Close() error = %v", err)
	}

	os.Stdout = originalStdout

	outputBytes, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("io.ReadAll() error = %v", err)
	}

	if err := reader.Close(); err != nil {
		t.Fatalf("reader.Close() error = %v", err)
	}

	return string(outputBytes)
}

// newParsedCommand creates a urfave/cli command, runs it with the provided
// arguments, and returns the parsed command instance passed into the action.
// This gives the tests a realistic command object with flag values populated.
func newParsedCommand(t *testing.T, flags []cli.Flag, arguments ...string) *cli.Command {
	t.Helper()

	var parsedCommand *cli.Command
	testCommand := &cli.Command{
		Name:  "test-command",
		Flags: flags,
		Action: func(_ context.Context, command *cli.Command) error {
			parsedCommand = command
			return nil
		},
	}

	runArguments := append([]string{"test-command"}, arguments...)
	if err := testCommand.Run(context.Background(), runArguments); err != nil {
		t.Fatalf("testCommand.Run() error = %v", err)
	}

	if parsedCommand == nil {
		t.Fatal("parsed command was not captured")
	}

	return parsedCommand
}

// newTeamIDsCommand builds a parsed command containing the flags used by
// prepareClientAndTeams, fetchAction, and downloadAction.
func newTeamIDsCommand(t *testing.T, arguments ...string) *cli.Command {
	t.Helper()

	return newParsedCommand(t, []cli.Flag{
		&cli.StringFlag{Name: "team-ids"},
		&cli.BoolFlag{Name: "json"},
		&cli.StringFlag{Name: "output"},
	}, arguments...)
}

// newSubscribeCommand builds a parsed command containing the subscription flags.
func newSubscribeCommand(t *testing.T, arguments ...string) *cli.Command {
	t.Helper()

	return newParsedCommand(t, []cli.Flag{
		&cli.StringFlag{Name: "team-id"},
		&cli.StringFlag{Name: "email"},
	}, arguments...)
}

// newCheckChangesCommand builds a parsed command containing the check-changes flag.
func newCheckChangesCommand(t *testing.T, arguments ...string) *cli.Command {
	t.Helper()

	return newParsedCommand(t, []cli.Flag{
		&cli.StringFlag{Name: "team-id"},
	}, arguments...)
}

func Test_main(t *testing.T) {
	originalArgs := os.Args
	t.Cleanup(func() {
		os.Args = originalArgs
	})

	os.Args = []string{"soccer-scraper", "--help"}

	output := captureStdout(t, func() {
		main()
	})

	if !strings.Contains(output, "Fetch soccer schedules and generate calendar files from the LPS API") {
		t.Fatalf("main() output missing CLI usage text: %q", output)
	}

	if !strings.Contains(output, "fetch") || !strings.Contains(output, "download") {
		t.Fatalf("main() output missing expected command names: %q", output)
	}
}

func Test_prepareClientAndTeams(t *testing.T) {
	tests := []struct {
		name                  string
		command               *cli.Command
		wantValidTeamIDs      []string
		wantInvalidTeamIDs    []app.InvalidTeamID
		wantErr               bool
		wantOutputContains    []string
		wantOutputNotContains []string
	}{
		{
			name:               "valid team IDs create a client",
			command:            newTeamIDsCommand(t, "--team-ids", "123456,654321"),
			wantValidTeamIDs:   []string{"123456", "654321"},
			wantInvalidTeamIDs: []app.InvalidTeamID{},
		},
		{
			name:    "mixed valid and invalid team IDs return partial results",
			command: newTeamIDsCommand(t, "--team-ids", "123456,123456,abcdef"),
			wantValidTeamIDs: []string{
				"123456",
			},
			wantInvalidTeamIDs: []app.InvalidTeamID{
				{ID: "123456", Reason: "Duplicate team ID"},
				{ID: "abcdef", Reason: "team ID 'abcdef' must be exactly 6 digits"},
			},
			wantOutputContains: []string{"Invalid team IDs:", "123456: Duplicate team ID", "abcdef: team ID 'abcdef' must be exactly 6 digits"},
		},
		{
			name:               "invalid-only team IDs return an error",
			command:            newTeamIDsCommand(t, "--team-ids", "abcdef"),
			wantErr:            true,
			wantOutputContains: []string{"Invalid team IDs:", "abcdef: team ID 'abcdef' must be exactly 6 digits"},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			var (
				gotClient         *lps.Client
				gotValidTeamIDs   []string
				gotInvalidTeamIDs []app.InvalidTeamID
				gotErr            error
			)

			output := captureStdout(t, func() {
				gotClient, gotValidTeamIDs, gotInvalidTeamIDs, gotErr = prepareClientAndTeams(testCase.command)
			})

			if (gotErr != nil) != testCase.wantErr {
				t.Fatalf("prepareClientAndTeams() error = %v, wantErr %v", gotErr, testCase.wantErr)
			}

			if testCase.wantErr {
				if gotClient != nil {
					t.Fatalf("prepareClientAndTeams() client = %v, want nil on error", gotClient)
				}
				if gotValidTeamIDs != nil {
					t.Fatalf("prepareClientAndTeams() valid team IDs = %v, want nil on error", gotValidTeamIDs)
				}
				if gotInvalidTeamIDs != nil {
					t.Fatalf("prepareClientAndTeams() invalid team IDs = %v, want nil on error", gotInvalidTeamIDs)
				}
			} else {
				if gotClient == nil {
					t.Fatal("prepareClientAndTeams() returned a nil client for valid input")
				}
				if !reflect.DeepEqual(gotValidTeamIDs, testCase.wantValidTeamIDs) {
					t.Fatalf("prepareClientAndTeams() valid team IDs = %v, want %v", gotValidTeamIDs, testCase.wantValidTeamIDs)
				}
				if !reflect.DeepEqual(gotInvalidTeamIDs, testCase.wantInvalidTeamIDs) {
					t.Fatalf("prepareClientAndTeams() invalid team IDs = %v, want %v", gotInvalidTeamIDs, testCase.wantInvalidTeamIDs)
				}
			}

			for _, expectedSubstring := range testCase.wantOutputContains {
				if !strings.Contains(output, expectedSubstring) {
					t.Fatalf("prepareClientAndTeams() output missing %q: %q", expectedSubstring, output)
				}
			}

			for _, unexpectedSubstring := range testCase.wantOutputNotContains {
				if strings.Contains(output, unexpectedSubstring) {
					t.Fatalf("prepareClientAndTeams() output unexpectedly contained %q: %q", unexpectedSubstring, output)
				}
			}
		})
	}
}

func Test_reportFailedTeams(t *testing.T) {
	tests := []struct {
		name               string
		failedTeams        []app.FailedTeam
		wantOutputContains []string
	}{
		{
			name: "prints each failed team",
			failedTeams: []app.FailedTeam{
				{TeamID: "123456", Error: "request timed out"},
				{TeamID: "654321", Error: "team not found"},
			},
			wantOutputContains: []string{
				"Failed to fetch some teams:",
				"Team 123456: request timed out",
				"Team 654321: team not found",
			},
		},
		{
			name:        "prints nothing for empty failures",
			failedTeams: nil,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			output := captureStdout(t, func() {
				reportFailedTeams(testCase.failedTeams)
			})

			if len(testCase.wantOutputContains) == 0 {
				if output != "" {
					t.Fatalf("reportFailedTeams() output = %q, want empty output", output)
				}
				return
			}

			for _, expectedSubstring := range testCase.wantOutputContains {
				if !strings.Contains(output, expectedSubstring) {
					t.Fatalf("reportFailedTeams() output missing %q: %q", expectedSubstring, output)
				}
			}
		})
	}
}

func Test_fetchAction(t *testing.T) {
	tests := []struct {
		name               string
		command            *cli.Command
		wantErrContains    string
		wantOutputContains []string
	}{
		{
			name:               "invalid team IDs fail before fetch",
			command:            newTeamIDsCommand(t, "--team-ids", "abcdef"),
			wantErrContains:    "no valid team IDs provided",
			wantOutputContains: []string{"Invalid team IDs:", "abcdef: team ID 'abcdef' must be exactly 6 digits"},
		},
		{
			name:            "empty team IDs fail validation",
			command:         newTeamIDsCommand(t),
			wantErrContains: "no valid team IDs provided",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			var gotErr error
			output := captureStdout(t, func() {
				gotErr = fetchAction(context.Background(), testCase.command)
			})

			if gotErr == nil {
				t.Fatal("fetchAction() error = nil, want non-nil error")
			}

			if !strings.Contains(gotErr.Error(), testCase.wantErrContains) {
				t.Fatalf("fetchAction() error = %v, want substring %q", gotErr, testCase.wantErrContains)
			}

			for _, expectedSubstring := range testCase.wantOutputContains {
				if !strings.Contains(output, expectedSubstring) {
					t.Fatalf("fetchAction() output missing %q: %q", expectedSubstring, output)
				}
			}
		})
	}
}

func Test_downloadAction(t *testing.T) {
	tests := []struct {
		name               string
		command            *cli.Command
		wantErrContains    string
		wantOutputContains []string
	}{
		{
			name:               "invalid team IDs fail before download",
			command:            newTeamIDsCommand(t, "--team-ids", "abcdef"),
			wantErrContains:    "no valid team IDs provided",
			wantOutputContains: []string{"Invalid team IDs:", "abcdef: team ID 'abcdef' must be exactly 6 digits"},
		},
		{
			name:            "empty team IDs fail validation",
			command:         newTeamIDsCommand(t),
			wantErrContains: "no valid team IDs provided",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			var gotErr error
			output := captureStdout(t, func() {
				gotErr = downloadAction(context.Background(), testCase.command)
			})

			if gotErr == nil {
				t.Fatal("downloadAction() error = nil, want non-nil error")
			}

			if !strings.Contains(gotErr.Error(), testCase.wantErrContains) {
				t.Fatalf("downloadAction() error = %v, want substring %q", gotErr, testCase.wantErrContains)
			}

			for _, expectedSubstring := range testCase.wantOutputContains {
				if !strings.Contains(output, expectedSubstring) {
					t.Fatalf("downloadAction() output missing %q: %q", expectedSubstring, output)
				}
			}
		})
	}
}

func Test_subscribeAction(t *testing.T) {
	tests := []struct {
		name            string
		command         *cli.Command
		wantErrContains string
	}{
		{
			name:            "invalid team ID is rejected",
			command:         newSubscribeCommand(t, "--team-id", "abc", "--email", "valid@example.com"),
			wantErrContains: "invalid team ID",
		},
		{
			name:            "invalid email is rejected before network work",
			command:         newSubscribeCommand(t, "--team-id", "123456", "--email", "not-an-email"),
			wantErrContains: "invalid email format",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			err := subscribeAction(context.Background(), testCase.command)
			if err == nil {
				t.Fatal("subscribeAction() error = nil, want non-nil error")
			}

			if !strings.Contains(err.Error(), testCase.wantErrContains) {
				t.Fatalf("subscribeAction() error = %v, want substring %q", err, testCase.wantErrContains)
			}
		})
	}
}

func Test_checkChangesAction(t *testing.T) {
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_REGION", "us-west-2")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	command := newCheckChangesCommand(t)
	err := checkChangesAction(ctx, command)
	if err == nil {
		t.Fatal("checkChangesAction() error = nil, want non-nil error for canceled context")
	}
}
