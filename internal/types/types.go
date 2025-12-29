package types

// Package types provide shared data structures used across the soccer
// schedule scraper application. These types are defined here to avoid
// import cycles between packages.

// Game represents a single soccer game with all relevant scheduling information.
// This structure is returned in fetch responses and consumed by download requests.
// Field names use JSON tags matching the original Python API contract.
type Game struct {
	// GameID is the unique identifier from the LPS API for this game.
	GameID string `json:"game_id"`

	// ID is a composite identifier for deduplication: season_date_home_away_field.
	// Generated during fetch to allow clients to identify unique games.
	ID string `json:"id"`

	// Date is the human-readable formatted date string (e.g., "Wed 01/15/25 07:00 PM").
	// Used for display purposes in the UI.
	Date string `json:"date"`

	// DateStr is the ISO 8601 formatted datetime string with timezone info.
	// Used for calendar event generation and precise time calculations.
	DateStr string `json:"date_str"`

	// Field is the field number or name where the game will be played.
	// Extracted from field_name or Field in the API response.
	Field string `json:"field"`

	// HomeTeam is the name of the home team for this game.
	HomeTeam string `json:"home_team"`

	// AwayTeam is the name of the visiting/away team for this game.
	AwayTeam string `json:"away_team"`

	// TeamID is the team ID queried to retrieve this game.
	// Added during fetch to track which team request returned this game.
	TeamID string `json:"team_id"`

	// Season is the season identifier (e.g., "2025") from the API response.
	Season string `json:"season"`

	// TeamName is the full team name from the API response.
	// This is the name of the team that was queried, not necessarily home/away.
	TeamName string `json:"team_name"`
}

// InvalidTeamID represents a team ID that failed validation with the reason.
// Used in error responses to provide detailed feedback about invalid inputs.
type InvalidTeamID struct {
	// ID is the invalid team ID string that was provided.
	ID string `json:"id"`

	// Reason describes why the team ID is invalid (e.g., "must be exactly 6 digits").
	Reason string `json:"reason"`
}

// FailedTeam represents a team ID that passed validation but failed during fetch.
// This allows partial success responses where some teams succeed and others fail.
type FailedTeam struct {
	// TeamID is the valid team ID that failed during the API fetch.
	TeamID string `json:"team_id"`

	// Error is the error message describing what went wrong.
	Error string `json:"error"`

	// ErrorType is the classification of the error (e.g., "RuntimeError", "ValueError").
	ErrorType string `json:"errorType"`
}
