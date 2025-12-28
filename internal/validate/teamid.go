package validate

// Package validate provides input validation functions for the soccer schedule
// scraper. It ensures team IDs and other inputs meet the required format before
// making API requests.

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/CraigDevJohnson/soccer_scraper/internal/types"
)

// teamIDPattern matches exactly 6 digits for valid team IDs.
// Team IDs from the LPS API are always 6-digit numeric strings.
var teamIDPattern = regexp.MustCompile(`^\d{6}$`)

// ValidateTeamID checks if a team ID string is properly formatted.
// A valid team ID must be:
//   - A non-empty string
//   - Exactly 6 digits
//   - A positive integer when parsed
//
// Returns nil if valid, or an error with a descriptive message if invalid.
func ValidateTeamID(teamID string) error {
	// Trim whitespace and check for empty string
	trimmed := strings.TrimSpace(teamID)
	if trimmed == "" {
		return fmt.Errorf("team ID cannot be empty")
	}

	// Check for 6-digit format
	if !teamIDPattern.MatchString(trimmed) {
		return fmt.Errorf("team ID '%s' must be exactly 6 digits", trimmed)
	}

	// Verify it's a positive integer (handles edge cases like all zeros)
	numID, err := strconv.Atoi(trimmed)
	if err != nil {
		return fmt.Errorf("team ID '%s' must be a valid number", trimmed)
	}
	if numID <= 0 {
		return fmt.Errorf("team ID '%s' must be a positive number", trimmed)
	}

	return nil
}

// ParseTeamIDsCSV parses a comma-separated string of team IDs, validates each one,
// and returns separate lists of valid and invalid team IDs. Duplicate team IDs
// are detected and added to the invalid list with a "Duplicate team ID" reason.
//
// This function mirrors the Python implementation's behavior of:
//   - Splitting on commas and trimming whitespace
//   - Validating each ID individually
//   - Tracking duplicates separately from validation errors
//   - Returning both valid and invalid lists for partial processing
//
// Parameters:
//   - input: Comma-separated string of team IDs (e.g., "123456,654321,123456")
//
// Returns:
//   - valid: Slice of validated, deduplicated team ID strings
//   - invalid: Slice of InvalidTeamID structs with ID and reason for each failure
func ParseTeamIDsCSV(input string) (valid []string, invalid []types.InvalidTeamID) {
	// Initialize empty slices (not nil) to match Python's empty list behavior
	valid = make([]string, 0)
	invalid = make([]types.InvalidTeamID, 0)

	// Handle empty input gracefully
	if strings.TrimSpace(input) == "" {
		return valid, invalid
	}

	// Split by comma and process each ID
	parts := strings.Split(input, ",")

	// Track seen IDs for duplicate detection
	seen := make(map[string]bool)

	for _, part := range parts {
		// Trim whitespace from each part
		teamID := strings.TrimSpace(part)

		// Skip empty parts (e.g., from "123,,456")
		if teamID == "" {
			continue
		}

		// Check for duplicates first (before validation)
		if seen[teamID] {
			invalid = append(invalid, types.InvalidTeamID{
				ID:     teamID,
				Reason: "Duplicate team ID",
			})
			continue
		}

		// Validate the team ID format
		if err := ValidateTeamID(teamID); err != nil {
			invalid = append(invalid, types.InvalidTeamID{
				ID:     teamID,
				Reason: err.Error(),
			})
			continue
		}

		// Mark as seen and add to valid list
		seen[teamID] = true
		valid = append(valid, teamID)
	}

	return valid, invalid
}
