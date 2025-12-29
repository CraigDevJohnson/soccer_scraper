package lps

// Package lps provides the HTTP client and types for interacting with the
// LPS (Let's Play Soccer) API. It handles fetching team schedules and parsing
// the API responses into internal game structures.

import (
	"github.com/CraigDevJohnson/soccer_scraper/internal/types"
)

// APIResponse represents the top-level response from the LPS team schedule API.
// The API returns team metadata and a list of games for the requested team.
type APIResponse struct {
	// Team contains metadata about the requested team.
	Team TeamData `json:"team"`

	// Games is the list of scheduled games for the team.
	Games []GameData `json:"games"`
}

// TeamData contains metadata about a team from the LPS API response.
type TeamData struct {
	// Season is the season number/identifier (e.g., 2025).
	Season int `json:"Season"`

	// TeamName is the full name of the team.
	TeamName string `json:"team_name"`
}

// GameData represents a single game from the LPS API response.
// Field names match the API's JSON structure.
type GameData struct {
	// GameID is the unique identifier for this game in the LPS system.
	GameID string `json:"game_id"`

	// SchedGameDateTime is the scheduled game time in ISO 8601 format.
	// NOTE: Despite the 'Z' suffix, the API returns times in local Mountain Time
	// (America/Denver), NOT UTC. The 'Z' suffix is incorrectly applied by the API.
	SchedGameDateTime string `json:"SchedGameDateTime"`

	// FieldName is the human-readable field name (e.g., "Field 1").
	// This takes precedence over the Field number if present.
	FieldName string `json:"field_name"`

	// Field is the numeric field identifier, used as fallback if FieldName is empty.
	Field int `json:"Field"`

	// HomeTeam contains information about the home team for this game.
	HomeTeam TeamInfo `json:"home_team"`

	// VisitorTeam contains information about the visiting/away team.
	VisitorTeam TeamInfo `json:"visitor_team"`
}

// TeamInfo contains basic information about a team in a game.
// Used for both home_team and visitor_team in the game data.
type TeamInfo struct {
	// TeamName is the name of the team.
	TeamName string `json:"team_name"`
}

// FetchResult contains the processed results from fetching a team's schedule.
// This is the internal representation returned by FetchTeamSchedule.
type FetchResult struct {
	// TeamID is the team ID that was fetched.
	TeamID string

	// Games is the list of processed games for this team.
	Games []ParsedGame

	// Season is the season identifier from the API response.
	Season string

	// TeamName is the team name from the API response.
	TeamName string

	// Error is set if the fetch failed, nil on success.
	Error error

	// ErrorType classifies the error (e.g., "RuntimeError", "ValueError").
	ErrorType string
}

// ParsedGame is the internal representation of a game after parsing from the API.
// It contains all the fields needed to create the public Game type.
type ParsedGame struct {
	// GameID is the unique game identifier from the API.
	GameID string

	// FormattedDate is the display date (e.g., "Sat 01/15 07:00 PM").
	FormattedDate string

	// ISODate is the ISO 8601 datetime string for calendar generation.
	ISODate string

	// Field is the field number/name where the game is played.
	Field string

	// HomeTeam is the home team name.
	HomeTeam string

	// AwayTeam is the away team name.
	AwayTeam string
}

// ConvertParsedGamesToTypesGames converts a slice of ParsedGame to types.Game.
// This helper function eliminates code duplication when converting parsed API
// data to the shared Game type used throughout the application.
//
// Parameters:
//   - games: Slice of ParsedGame structs from the LPS client
//
// Returns:
//   - []types.Game: Slice of Game structs for use in comparison and storage
func ConvertParsedGamesToTypesGames(games []ParsedGame) []types.Game {
	result := make([]types.Game, len(games))
	for i, pg := range games {
		result[i] = types.Game{
			GameID:   pg.GameID,
			DateStr:  pg.ISODate,
			Field:    pg.Field,
			HomeTeam: pg.HomeTeam,
			AwayTeam: pg.AwayTeam,
		}
	}
	return result
}
