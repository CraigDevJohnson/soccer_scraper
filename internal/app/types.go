package app

// Package app provides the core application types and handler logic for the
// soccer schedule scraper. It defines request/response structures that match
// the API contract from the original Python implementation.

import (
	"github.com/CraigDevJohnson/soccer_scraper/internal/types"
)

// Version identifier for logging and debugging purposes.
// This should be updated with each release to track deployed versions.
const Version = "2025-03-03-go-v1"

// Action constants define the supported API actions that can be requested
// via the 'action' query parameter. These mirror the Python implementation.
const (
	// ActionFetch retrieves game schedules for the specified team IDs.
	// Returns JSON with games array and optional error details.
	ActionFetch = "fetch"

	// ActionDownload generates an ICS calendar file from provided games.
	// Expects POST body with games array, returns text/calendar content.
	ActionDownload = "download"
)

// Type aliases for shared types to simplify usage within this package.
// These allow other packages to use app.Game instead of types.Game.
type (
	// Game represents a single soccer game with all relevant scheduling information.
	Game = types.Game

	// InvalidTeamID represents a team ID that failed validation with the reason.
	InvalidTeamID = types.InvalidTeamID

	// FailedTeam represents a team ID that passed validation but failed during fetch.
	FailedTeam = types.FailedTeam
)

// FetchResponse is the JSON response body for the fetch action.
// It includes successfully retrieved games and detailed error information
// for any teams that failed validation or fetch operations.
type FetchResponse struct {
	// Games is the list of all successfully retrieved games across all valid team IDs.
	Games []Game `json:"games"`

	// ProcessedTeamIDs lists all team IDs that passed validation and were attempted.
	ProcessedTeamIDs []string `json:"processed_team_ids"`

	// FailedTeams lists teams that passed validation but failed during fetch.
	// Only present if there were fetch failures.
	FailedTeams []FailedTeam `json:"failed_teams,omitempty"`

	// InvalidTeamIDs lists team IDs that failed validation.
	// Only present if there were validation failures.
	InvalidTeamIDs []InvalidTeamID `json:"invalid_team_ids,omitempty"`
}

// ErrorResponse is the JSON response body for error conditions.
// Used for 400/500 status responses with detailed error information.
type ErrorResponse struct {
	// Error is the primary error message for the client.
	Error string `json:"error"`

	// ErrorType classifies the error (e.g., "ValidationError", "RuntimeError").
	ErrorType string `json:"errorType"`

	// ProcessedTeamIDs lists team IDs that were attempted before the error.
	// Only present for partial failure scenarios.
	ProcessedTeamIDs []string `json:"processed_team_ids,omitempty"`

	// FailedTeams lists teams that failed during processing.
	// Only present if there were individual team failures.
	FailedTeams []FailedTeam `json:"failed_teams,omitempty"`

	// InvalidTeamIDs lists team IDs that failed validation.
	// Only present if there were validation failures.
	InvalidTeamIDs []InvalidTeamID `json:"invalid_team_ids,omitempty"`

	// ValidationErrors lists all validation error messages.
	// Only present for validation error responses.
	ValidationErrors []string `json:"validation_errors,omitempty"`
}

// DownloadRequest is the expected JSON body for the download action.
// Clients POST this structure to generate an ICS calendar file.
type DownloadRequest struct {
	// Games is the list of games to include in the calendar file.
	Games []Game `json:"games"`
}
