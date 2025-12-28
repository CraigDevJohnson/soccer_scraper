package main

// Package main provides a CLI interface for the soccer schedule scraper.
// This allows local testing of the same functionality that runs in Lambda,
// using urfave/cli for command handling.

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/mail"
	"os"
	"strings"

	"github.com/CraigDevJohnson/soccer_scraper/internal/app"
	"github.com/CraigDevJohnson/soccer_scraper/internal/calendar"
	"github.com/CraigDevJohnson/soccer_scraper/internal/lps"
	"github.com/CraigDevJohnson/soccer_scraper/internal/notify"
	"github.com/CraigDevJohnson/soccer_scraper/internal/sns"
	"github.com/CraigDevJohnson/soccer_scraper/internal/storage"
	"github.com/CraigDevJohnson/soccer_scraper/internal/validate"
	"github.com/urfave/cli/v2"
)

func main() {
	// Create the CLI application with urfave/cli
	cliApp := &cli.App{
		Name:    "soccer-scraper",
		Usage:   "Fetch soccer schedules and generate calendar files from the LPS API",
		Version: app.Version,
		Commands: []*cli.Command{
			{
				Name:    "fetch",
				Aliases: []string{"f"},
				Usage:   "Fetch game schedules for one or more team IDs",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "team-ids",
						Aliases:  []string{"t"},
						Usage:    "Comma-separated list of 6-digit team IDs (e.g., 123456,654321)",
						Required: true,
					},
					&cli.BoolFlag{
						Name:    "json",
						Aliases: []string{"j"},
						Usage:   "Output results as JSON instead of formatted text",
					},
				},
				Action: fetchAction,
			},
			{
				Name:    "download",
				Aliases: []string{"d"},
				Usage:   "Generate an ICS calendar file from fetched games",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "team-ids",
						Aliases:  []string{"t"},
						Usage:    "Comma-separated list of 6-digit team IDs (e.g., 123456,654321)",
						Required: true,
					},
					&cli.StringFlag{
						Name:    "output",
						Aliases: []string{"o"},
						Usage:   "Output filename for the ICS file (default: auto-generated)",
					},
				},
				Action: downloadAction,
			},
			{
				Name:    "subscribe",
				Aliases: []string{"s"},
				Usage:   "Subscribe to email notifications for schedule changes",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "team-id",
						Aliases:  []string{"t"},
						Usage:    "6-digit team ID to subscribe to (e.g., 469306)",
						Required: true,
					},
					&cli.StringFlag{
						Name:     "email",
						Aliases:  []string{"e"},
						Usage:    "Email address to receive notifications",
						Required: true,
					},
				},
				Action: subscribeAction,
			},
			{
				Name:    "check-changes",
				Aliases: []string{"c"},
				Usage:   "Check for schedule changes and send notifications (for scheduled runs)",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "team-id",
						Aliases: []string{"t"},
						Usage:   "Specific team ID to check (optional, checks all if not provided)",
					},
				},
				Action: checkChangesAction,
			},
		},
	}

	// Run the CLI application
	if err := cliApp.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

// fetchAction handles the 'fetch' command to retrieve game schedules.
func fetchAction(c *cli.Context) error {
	// Parse and validate team IDs
	teamIDsParam := c.String("team-ids")
	validTeamIDs, invalidTeamIDs := validate.ParseTeamIDsCSV(teamIDsParam)

	// Report any invalid team IDs
	if len(invalidTeamIDs) > 0 {
		fmt.Println("\nInvalid team IDs:")
		for _, inv := range invalidTeamIDs {
			fmt.Printf("  - %s: %s\n", inv.ID, inv.Reason)
		}
	}

	// Exit if no valid team IDs
	if len(validTeamIDs) == 0 {
		return fmt.Errorf("no valid team IDs provided")
	}

	// Create LPS client
	client, err := lps.NewClient()
	if err != nil {
		return fmt.Errorf("failed to create LPS client: %w", err)
	}

	// Fetch games concurrently for all valid team IDs
	fmt.Printf("\nFetching schedules for %d team(s)...\n", len(validTeamIDs))
	games, failedTeams := client.FetchMultipleTeams(context.Background(), validTeamIDs)

	// Report any failed teams
	if len(failedTeams) > 0 {
		fmt.Println("\nFailed to fetch some teams:")
		for _, ft := range failedTeams {
			fmt.Printf("  - Team %s: %s\n", ft.TeamID, ft.Error)
		}
	}

	// Output results
	if c.Bool("json") {
		// JSON output mode
		response := app.FetchResponse{
			Games:            games,
			ProcessedTeamIDs: validTeamIDs,
		}
		if len(failedTeams) > 0 {
			response.FailedTeams = failedTeams
		}
		if len(invalidTeamIDs) > 0 {
			response.InvalidTeamIDs = invalidTeamIDs
		}

		jsonOutput, err := json.MarshalIndent(response, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal JSON: %w", err)
		}
		fmt.Println(string(jsonOutput))
	} else {
		// Formatted text output
		if len(games) == 0 {
			fmt.Println("\nNo upcoming games found.")
			return nil
		}

		fmt.Printf("\nFound %d upcoming game(s):\n", len(games))
		fmt.Println(strings.Repeat("-", 60))

		for _, game := range games {
			fmt.Printf("\nDate/Time: %s\n", game.Date)
			fmt.Printf("Field:     %s\n", game.Field)
			fmt.Printf("Home Team: %s\n", game.HomeTeam)
			fmt.Printf("Away Team: %s\n", game.AwayTeam)
			fmt.Printf("Team:      %s (ID: %s)\n", game.TeamName, game.TeamID)
			fmt.Printf("Season:    %s\n", game.Season)
			fmt.Println(strings.Repeat("-", 40))
		}
	}

	return nil
}

// downloadAction handles the 'download' command to generate an ICS calendar file.
func downloadAction(c *cli.Context) error {
	// Parse and validate team IDs
	teamIDsParam := c.String("team-ids")
	validTeamIDs, invalidTeamIDs := validate.ParseTeamIDsCSV(teamIDsParam)

	// Report any invalid team IDs
	if len(invalidTeamIDs) > 0 {
		fmt.Println("\nInvalid team IDs:")
		for _, inv := range invalidTeamIDs {
			fmt.Printf("  - %s: %s\n", inv.ID, inv.Reason)
		}
	}

	// Exit if no valid team IDs
	if len(validTeamIDs) == 0 {
		return fmt.Errorf("no valid team IDs provided")
	}

	// Create LPS client
	client, err := lps.NewClient()
	if err != nil {
		return fmt.Errorf("failed to create LPS client: %w", err)
	}

	// Fetch games concurrently for all valid team IDs
	fmt.Printf("\nFetching schedules for %d team(s)...\n", len(validTeamIDs))
	games, failedTeams := client.FetchMultipleTeams(context.Background(), validTeamIDs)

	// Report any failed teams
	if len(failedTeams) > 0 {
		fmt.Println("\nFailed to fetch some teams:")
		for _, ft := range failedTeams {
			fmt.Printf("  - Team %s: %s\n", ft.TeamID, ft.Error)
		}
	}

	// Exit if no games found
	if len(games) == 0 {
		return fmt.Errorf("no upcoming games found")
	}

	fmt.Printf("Found %d upcoming game(s)\n", len(games))

	// Create ICS generator
	icsGen, err := calendar.NewGenerator()
	if err != nil {
		return fmt.Errorf("failed to create ICS generator: %w", err)
	}

	// Convert app.Game to calendar.GameEvent for ICS generation
	gameEvents := make([]calendar.GameEvent, len(games))
	for i, g := range games {
		gameEvents[i] = calendar.GameEvent{
			GameID:   g.GameID,
			DateStr:  g.DateStr,
			Field:    g.Field,
			HomeTeam: g.HomeTeam,
			AwayTeam: g.AwayTeam,
			Season:   g.Season,
			TeamName: g.TeamName,
			TeamID:   g.TeamID,
		}
	}

	// Generate ICS content
	icsContent, err := icsGen.GenerateICS(gameEvents)
	if err != nil {
		return fmt.Errorf("failed to generate calendar: %w", err)
	}

	// Determine output filename
	filename := c.String("output")
	if filename == "" {
		// Auto-generate filename from first game's data
		if len(games) > 0 {
			firstGame := games[0]
			filename = calendar.GetFilename(firstGame.Season, firstGame.TeamName, firstGame.TeamID)
		} else {
			filename = "soccer_schedule.ics"
		}
	}

	// Write to file with proper line endings (CRLF for ICS)
	err = os.WriteFile(filename, []byte(icsContent), 0644)
	if err != nil {
		return fmt.Errorf("failed to write calendar file: %w", err)
	}

	fmt.Printf("\nCalendar file '%s' created successfully!\n", filename)
	fmt.Printf("Contains %d game(s) from %d team(s)\n", len(games), len(validTeamIDs))

	return nil
}

// subscribeAction handles the 'subscribe' command to add email notifications.
// It creates an SNS topic for the team if needed, subscribes the email,
// and stores the initial schedule in DynamoDB for future comparison.
func subscribeAction(c *cli.Context) error {
	ctx := context.Background()

	// Validate team ID
	teamIDParam := c.String("team-id")
	if err := validate.ValidateTeamID(teamIDParam); err != nil {
		return fmt.Errorf("invalid team ID: %w", err)
	}

	// Validate email using net/mail.ParseAddress (RFC 5322 compliant)
	email := c.String("email")
	parsedEmail, err := mail.ParseAddress(email)
	if err != nil {
		return fmt.Errorf("invalid email format '%s': %w", email, err)
	}
	// Use the parsed address (handles cases like "Name <email@example.com>")
	email = parsedEmail.Address

	fmt.Printf("\nSubscribing %s to schedule updates for team %s...\n", email, teamIDParam)

	// Create LPS client and fetch current schedule
	lpsClient, err := lps.NewClient()
	if err != nil {
		return fmt.Errorf("failed to create LPS client: %w", err)
	}

	fmt.Println("Fetching current schedule...")
	result := lpsClient.FetchTeamSchedule(ctx, teamIDParam)
	if result.Error != nil {
		return fmt.Errorf("failed to fetch schedule: %w", result.Error)
	}

	fmt.Printf("Found %d games for %s (Season: %s)\n", len(result.Games), result.TeamName, result.Season)

	// Create SNS client and topic
	snsClient, err := sns.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create SNS client: %w", err)
	}

	fmt.Println("Creating/getting notification topic...")
	topicArn, err := snsClient.GetOrCreateTopic(ctx, teamIDParam)
	if err != nil {
		return fmt.Errorf("failed to create topic: %w", err)
	}

	// Subscribe email to topic
	fmt.Println("Subscribing email to topic...")
	subArn, err := snsClient.SubscribeEmail(ctx, topicArn, email)
	if err != nil {
		return fmt.Errorf("failed to subscribe email: %w", err)
	}

	// Create storage client and save schedule
	storageClient, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create storage client: %w", err)
	}

	// Convert games to stored format
	var games []storage.StoredGame
	for _, g := range result.Games {
		games = append(games, storage.StoredGame{
			GameID:   g.GameID,
			DateStr:  g.ISODate,
			Field:    g.Field,
			HomeTeam: g.HomeTeam,
			AwayTeam: g.AwayTeam,
		})
	}

	// Save schedule for future comparison
	fmt.Println("Saving schedule for change tracking...")
	schedule := &storage.StoredSchedule{
		TeamID:   teamIDParam,
		TeamName: result.TeamName,
		Season:   result.Season,
		Games:    games,
		TopicArn: topicArn,
	}
	err = storageClient.SaveSchedule(ctx, schedule)
	if err != nil {
		return fmt.Errorf("failed to save schedule: %w", err)
	}

	fmt.Println(strings.Repeat("=", 50))
	fmt.Println("Subscription setup complete!")
	fmt.Println(strings.Repeat("=", 50))
	fmt.Printf("\nTeam:         %s (ID: %s)\n", result.TeamName, teamIDParam)
	fmt.Printf("Email:        %s\n", email)
	fmt.Printf("Subscription: %s\n", subArn)
	fmt.Printf("Topic ARN:    %s\n", topicArn)
	fmt.Printf("Games stored: %d\n", len(games))
	fmt.Println("\n⚠️  IMPORTANT: Check your email for a confirmation message.")
	fmt.Println("   You must click the confirmation link to receive notifications.")
	fmt.Println("\nYou will be notified when:")
	fmt.Println("  - Games are added to the schedule")
	fmt.Println("  - Games are removed/cancelled")
	fmt.Println("  - Game times or fields change")

	return nil
}

// checkChangesAction handles the 'check-changes' command to detect and notify schedule changes.
// This can check a specific team or all subscribed teams.
func checkChangesAction(c *cli.Context) error {
	ctx := context.Background()

	// Create all required clients
	lpsClient, err := lps.NewClient()
	if err != nil {
		return fmt.Errorf("failed to create LPS client: %w", err)
	}

	snsClient, err := sns.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create SNS client: %w", err)
	}

	storageClient, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create storage client: %w", err)
	}

	// Create the checker
	checker := notify.NewChecker(lpsClient, storageClient, snsClient)

	// Check specific team or all teams
	teamID := c.String("team-id")
	if teamID != "" {
		// Validate team ID
		if err := validate.ValidateTeamID(teamID); err != nil {
			return fmt.Errorf("invalid team ID: %w", err)
		}

		fmt.Printf("\nChecking schedule changes for team %s...\n", teamID)
		hasChanges, err := checker.CheckAndNotify(ctx, teamID)
		if err != nil {
			return fmt.Errorf("failed to check team %s: %w", teamID, err)
		}

		if hasChanges {
			fmt.Println("✅ Schedule changes detected and notifications sent!")
		} else {
			fmt.Println("No schedule changes detected.")
		}
	} else {
		// Check all teams
		fmt.Println("\nChecking schedule changes for all subscribed teams...")
		changesCount, totalChecked, err := checker.CheckAllTeams(ctx)
		if err != nil {
			return fmt.Errorf("failed to check teams: %w", err)
		}

		fmt.Println(strings.Repeat("=", 50))
		fmt.Printf("Schedule check complete!\n")
		fmt.Printf("Teams checked: %d\n", totalChecked)
		fmt.Printf("Teams with changes: %d\n", changesCount)
	}

	return nil
}
