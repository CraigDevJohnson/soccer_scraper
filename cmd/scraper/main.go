package main

// Package main provides a CLI interface for the soccer schedule scraper.
// This allows local testing of the same functionality that runs in Lambda,
// using urfave/cli for command handling.

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/CraigDevJohnson/soccer_scraper/internal/app"
	"github.com/CraigDevJohnson/soccer_scraper/internal/calendar"
	"github.com/CraigDevJohnson/soccer_scraper/internal/lps"
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
