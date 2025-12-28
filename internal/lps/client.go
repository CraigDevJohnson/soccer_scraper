package lps

// Package lps provides the HTTP client for fetching team schedules from the
// LPS (Let's Play Soccer) API with support for concurrent requests and proper
// timeout handling.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	// Embed timezone data for reliable America/Denver loading in Lambda
	_ "time/tzdata"

	"github.com/CraigDevJohnson/soccer_scraper/internal/types"
	"golang.org/x/sync/errgroup"
)

// Client configuration constants for API requests.
const (
	// BaseURL is the LPS API endpoint for team schedule data.
	BaseURL = "https://lps-api-prod.lps-test.com/teams"

	// RequestTimeout is the maximum time to wait for a single API request.
	RequestTimeout = 10 * time.Second

	// MaxConcurrentRequests is the fixed limit for parallel team fetches.
	// This prevents overwhelming the API and respects Lambda CPU constraints.
	MaxConcurrentRequests = 8
)

// Client handles HTTP requests to the LPS API with proper timeout and
// connection pooling. It should be reused across requests for efficiency.
type Client struct {
	// httpClient is the underlying HTTP client with connection pooling.
	httpClient *http.Client

	// location is the America/Denver timezone for proper time handling.
	location *time.Location
}

// NewClient creates a new LPS API client with configured timeouts and
// the America/Denver timezone loaded for proper game time handling.
//
// Returns an error if the timezone cannot be loaded (should not happen
// with embedded tzdata, but handled defensively).
func NewClient() (*Client, error) {
	// Load America/Denver timezone for proper Mountain Time handling
	loc, err := time.LoadLocation("America/Denver")
	if err != nil {
		return nil, fmt.Errorf("failed to load America/Denver timezone: %w", err)
	}

	// Create HTTP client with timeout and connection pooling
	httpClient := &http.Client{
		Timeout: RequestTimeout,
		Transport: &http.Transport{
			MaxIdleConns:        10,
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     30 * time.Second,
		},
	}

	return &Client{
		httpClient: httpClient,
		location:   loc,
	}, nil
}

// FetchTeamSchedule retrieves the schedule for a single team from the LPS API.
// It validates the response structure, parses games, and filters to upcoming
// games only (games in the future relative to the current time).
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - teamID: The 6-digit team ID to fetch (should be pre-validated)
//
// Returns:
//   - FetchResult containing games, season, team name, or error details
func (c *Client) FetchTeamSchedule(ctx context.Context, teamID string) FetchResult {
	result := FetchResult{TeamID: teamID}

	// Build the API URL for this team
	url := fmt.Sprintf("%s/%s", BaseURL, teamID)

	// Create request with context for timeout/cancellation
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		result.Error = fmt.Errorf("failed to create request: %w", err)
		result.ErrorType = "RuntimeError"
		return result
	}

	// Execute the HTTP request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		// Classify the error type for better client feedback
		if strings.Contains(err.Error(), "deadline exceeded") || strings.Contains(err.Error(), "timeout") {
			result.Error = fmt.Errorf("request timed out while fetching schedule for team %s; please try again", teamID)
		} else if strings.Contains(err.Error(), "connection") {
			result.Error = fmt.Errorf("connection error while fetching schedule for team %s; please check your internet connection", teamID)
		} else {
			result.Error = fmt.Errorf("could not fetch any schedule, make sure team ID is accurate")
		}
		result.ErrorType = "RuntimeError"
		return result
	}
	defer resp.Body.Close()

	// Check for non-2xx status codes
	if resp.StatusCode != http.StatusOK {
		result.Error = fmt.Errorf("could not fetch any schedule, make sure team ID is accurate")
		result.ErrorType = "RuntimeError"
		return result
	}

	// Parse JSON response
	var apiResp APIResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		result.Error = fmt.Errorf("failed to parse API response for team %s: %v", teamID, err)
		result.ErrorType = "RuntimeError"
		return result
	}

	// Validate response structure - check for team data
	if apiResp.Team.TeamName == "" {
		result.Error = fmt.Errorf("team ID %s not found, please verify the team code is correct", teamID)
		result.ErrorType = "ValueError"
		return result
	}

	// Extract season and team name
	result.Season = strconv.Itoa(apiResp.Team.Season)
	if result.Season == "0" {
		result.Season = "Unknown"
	}
	result.TeamName = apiResp.Team.TeamName
	if result.TeamName == "" {
		result.TeamName = "Unknown Team"
	}

	// Debug logging (matches Python behavior)
	fmt.Printf("Team Name: %s, Season: %s\n", result.TeamName, result.Season)

	// Check for games data
	if len(apiResp.Games) == 0 {
		result.Error = fmt.Errorf("no games data found for team %s", teamID)
		result.ErrorType = "ValueError"
		return result
	}

	// Parse games and filter to upcoming only
	games, err := c.parseGames(apiResp.Games)
	if err != nil {
		result.Error = err
		result.ErrorType = "ValueError"
		return result
	}

	if len(games) == 0 {
		result.Error = fmt.Errorf("no upcoming games found for the provided team")
		result.ErrorType = "ValueError"
		return result
	}

	result.Games = games
	fmt.Printf("Found %d games for team %s\n", len(games), teamID)

	return result
}

// parseGames converts API game data to internal ParsedGame structs,
// filtering to only include games that are in the future.
func (c *Client) parseGames(apiGames []GameData) ([]ParsedGame, error) {
	// Get current time in Mountain Time, truncated to minute for comparison
	now := time.Now().In(c.location).Truncate(time.Minute)
	fmt.Printf("Current date: %s\n", now.Format(time.RFC3339))

	games := make([]ParsedGame, 0, len(apiGames))

	for _, g := range apiGames {
		// Skip games missing required fields
		if g.SchedGameDateTime == "" {
			fmt.Printf("Warning: Missing game datetime for game in response\n")
			continue
		}
		if g.HomeTeam.TeamName == "" || g.VisitorTeam.TeamName == "" {
			fmt.Printf("Warning: Missing team data for game\n")
			continue
		}

		// Extract field name - prefer FieldName, fall back to Field number
		field := strings.TrimPrefix(g.FieldName, "Field ")
		if field == "" {
			field = strconv.Itoa(g.Field)
		}
		if field == "0" {
			fmt.Printf("Warning: Missing field data for game\n")
			continue
		}

		// Parse the game datetime - API returns UTC times with 'Z' suffix
		// We parse as UTC then convert to Mountain Time for proper wall clock time
		gameTime, err := c.parseGameTime(g.SchedGameDateTime)
		if err != nil {
			fmt.Printf("Warning: Error parsing date for game: %s - %v\n", g.SchedGameDateTime, err)
			continue
		}

		// Filter to future games only
		if gameTime.Before(now) {
			continue
		}

		// Format date for display (matches Python: "Sat 01/15 07:00 PM")
		formattedDate := gameTime.Format("Mon 01/02 03:04 PM")

		// ISO format for calendar generation
		isoDate := gameTime.Format(time.RFC3339)

		games = append(games, ParsedGame{
			GameID:        g.GameID,
			FormattedDate: formattedDate,
			ISODate:       isoDate,
			Field:         field,
			HomeTeam:      g.HomeTeam.TeamName,
			AwayTeam:      g.VisitorTeam.TeamName,
		})
	}

	return games, nil
}

// parseGameTime parses an API datetime string and interprets it as Mountain Time.
// IMPORTANT: Despite the 'Z' suffix in the API response, the times are actually
// in local Mountain Time (America/Denver), NOT UTC. The API incorrectly formats
// local times with a 'Z' suffix. We strip any timezone info and interpret the
// time values directly as Mountain Time to get correct wall clock times.
func (c *Client) parseGameTime(dateStr string) (time.Time, error) {
	// Strip the 'Z' suffix if present - the API incorrectly uses 'Z' for local times
	dateStr = strings.TrimSuffix(dateStr, "Z")

	// Also strip any timezone offset suffix (e.g., "+00:00", "-07:00")
	// We only want the local time portion: "2006-01-02T15:04:05"
	if idx := strings.LastIndex(dateStr, "+"); idx > 10 {
		dateStr = dateStr[:idx]
	} else if idx := strings.LastIndex(dateStr, "-"); idx > 10 {
		dateStr = dateStr[:idx]
	}

	// Parse the time without timezone - these are the raw local time values
	t, err := time.Parse("2006-01-02T15:04:05", dateStr)
	if err != nil {
		return time.Time{}, fmt.Errorf("unable to parse datetime: %s", dateStr)
	}

	// Interpret the parsed time as Mountain Time (the API's actual timezone)
	// time.Date creates a new time in the specified location with the same clock values
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), 0, c.location), nil
}

// FetchMultipleTeams fetches schedules for multiple team IDs concurrently.
// It uses bounded parallelism to prevent overwhelming the API and collects
// results from all teams, allowing partial success (some teams may fail
// while others succeed).
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - teamIDs: Slice of validated team ID strings to fetch
//
// Returns:
//   - []types.Game: All successfully retrieved games across all teams
//   - []types.FailedTeam: Details of any teams that failed to fetch
func (c *Client) FetchMultipleTeams(ctx context.Context, teamIDs []string) ([]types.Game, []types.FailedTeam) {
	// Use mutex to safely collect results from concurrent goroutines
	var mu sync.Mutex
	allGames := make([]types.Game, 0)
	failedTeams := make([]types.FailedTeam, 0)

	// Create errgroup with bounded concurrency
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(MaxConcurrentRequests)

	for _, teamID := range teamIDs {
		// Capture teamID for goroutine closure
		teamID := teamID

		g.Go(func() error {
			// Fetch this team's schedule
			result := c.FetchTeamSchedule(ctx, teamID)

			// Lock to safely update shared slices
			mu.Lock()
			defer mu.Unlock()

			if result.Error != nil {
				// Record the failure but don't stop other fetches
				failedTeams = append(failedTeams, types.FailedTeam{
					TeamID:    teamID,
					Error:     result.Error.Error(),
					ErrorType: result.ErrorType,
				})
				// Return nil to allow other fetches to continue
				return nil
			}

			// Convert ParsedGames to types.Game and add to results
			for _, pg := range result.Games {
				game := types.Game{
					GameID:   pg.GameID,
					Date:     pg.FormattedDate,
					DateStr:  pg.ISODate,
					Field:    pg.Field,
					HomeTeam: pg.HomeTeam,
					AwayTeam: pg.AwayTeam,
					TeamID:   teamID,
					Season:   result.Season,
					TeamName: result.TeamName,
					// Generate composite ID for deduplication
					ID: fmt.Sprintf("%s_%s_%s_%s_%s", result.Season, pg.FormattedDate, pg.HomeTeam, pg.AwayTeam, pg.Field),
				}
				allGames = append(allGames, game)
			}

			return nil
		})
	}

	// Wait for all fetches to complete (errors are collected, not returned)
	_ = g.Wait()

	return allGames, failedTeams
}
