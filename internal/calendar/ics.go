package calendar

// Package calendar provides ICS (iCalendar) file generation for soccer games.
// It creates properly formatted calendar events with Mountain Time timezone
// handling, alarms, and special event detection.

import (
	"fmt"
	"strings"
	"time"

	"github.com/CraigDevJohnson/soccer_scraper/internal/types"

	// Embed timezone data for reliable America/Denver loading in Lambda
	_ "time/tzdata"

	ics "github.com/arran4/golang-ical"
)

// GameEvent contains the fields needed to create a calendar event.
// This is a standalone type to avoid import cycles with the app package.
type GameEvent struct {
	// GameID is the unique identifier for the game.
	GameID string

	// DateStr is the ISO 8601 datetime string (RFC3339 format).
	DateStr string

	// Field is the field number/name where the game is played.
	Field string

	// HomeTeam is the home team name.
	HomeTeam string

	// AwayTeam is the away team name.
	AwayTeam string

	// Season is the season identifier (used for filename generation).
	Season string

	// TeamName is the team name (used for filename generation).
	TeamName string

	// TeamID is the team ID (used for filename generation).
	TeamID string
}

// Configuration constants for calendar event generation.
const (
	// DefaultDuration is the standard game length (45 minutes).
	DefaultDuration = 45 * time.Minute

	// AlarmTrigger is how long before the event to trigger the reminder (as duration string).
	AlarmTrigger = "-PT40M"

	// FixedLocation is the venue address for all games.
	FixedLocation = "Let's Play Soccer, Boise, 11448 W President Dr #8967, Boise, ID 83713, USA"

	// TimezoneName is the IANA timezone identifier for Mountain Time.
	TimezoneName = "America/Denver"
)

// SpecialTeams is the list of team names that trigger special event formatting.
// Matches are case-sensitive, matching the Python implementation.
var SpecialTeams = map[string]bool{
	"EYE CANDY": true,
}

// Generator creates ICS calendar files from game data with proper timezone
// handling and event formatting.
type Generator struct {
	// location is the America/Denver timezone for event times.
	location *time.Location
}

// NewGenerator creates a new ICS generator with the America/Denver timezone loaded.
// Returns an error if the timezone cannot be loaded (defensive, should not happen
// with embedded tzdata).
func NewGenerator() (*Generator, error) {
	loc, err := time.LoadLocation(TimezoneName)
	if err != nil {
		return nil, fmt.Errorf("failed to load %s timezone: %w", TimezoneName, err)
	}

	return &Generator{
		location: loc,
	}, nil
}

// FromAppGames converts a slice of types.Game to a slice of GameEvent.
// This helper centralizes the conversion logic used by both the Lambda handler
// and the CLI scraper tool.
//
// Parameters:
//   - games: Slice of Game structs from the app package
//
// Returns:
//   - []GameEvent: Slice of GameEvent structs for ICS generation
func FromAppGames(games []types.Game) []GameEvent {
	events := make([]GameEvent, len(games))
	for i, g := range games {
		events[i] = GameEvent{
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
	return events
}

// GenerateICS creates an ICS calendar string from a list of games.
// Each game becomes a VEVENT with:
//   - Proper DTSTART with TZID=America/Denver
//   - 45-minute duration
//   - Fixed venue location
//   - Description with field and team info
//   - 40-minute reminder alarm
//   - Special event formatting for designated teams
//
// Parameters:
//   - games: Slice of GameEvent structs containing schedule information
//
// Returns:
//   - string: The complete ICS calendar content
//   - error: Any error during generation
func (g *Generator) GenerateICS(games []GameEvent) (string, error) {
	// Create a new calendar with proper settings
	cal := ics.NewCalendarFor("Soccer Schedule API")
	cal.SetMethod(ics.MethodPublish)
	cal.SetCalscale("GREGORIAN")

	// Add the VTIMEZONE component for America/Denver with DST rules
	g.addTimezone(cal)

	// Process each game into an event
	for i, game := range games {
		err := g.addEvent(cal, game, i)
		if err != nil {
			// Log warning but continue with other games
			fmt.Printf("Warning: Error creating event for game %s: %v\n", game.GameID, err)
			continue
		}
	}

	return cal.Serialize(), nil
}

// addTimezone adds the VTIMEZONE component for America/Denver with DST rules.
// This ensures calendar clients properly interpret the event times.
// Uses Mountain Time with:
//   - STANDARD: MST (UTC-7) starting the first Sunday of November
//   - DAYLIGHT: MDT (UTC-6) starting second Sunday of March
func (g *Generator) addTimezone(cal *ics.Calendar) {
	// Add timezone definition using the library's built-in method
	tz := cal.AddTimezone(TimezoneName)

	// Add STANDARD component (winter time: MST, UTC-7)
	// Transition: First Sunday of November at 2:00 AM local time
	standard := tz.AddStandard()
	standard.AddProperty(ics.ComponentProperty(ics.PropertyDtstart), "19701101T020000")
	standard.AddProperty(ics.ComponentProperty(ics.PropertyTzoffsetfrom), "-0600")
	standard.AddProperty(ics.ComponentProperty(ics.PropertyTzoffsetto), "-0700")
	standard.AddProperty(ics.ComponentProperty(ics.PropertyRrule), "FREQ=YEARLY;BYMONTH=11;BYDAY=1SU")
	standard.AddProperty(ics.ComponentProperty(ics.PropertyTzname), "MST")

	// Add a DAYLIGHT component (summer: MDT, UTC-6)
	// Transition: Second Sunday of March at 2:00 AM local time
	// Note: golang-ical v0.3.2 doesn't have AddDaylight(), so we create it manually
	// and append to the VTimezone.Components slice (which is exposed via ComponentBase)
	daylight := &ics.Daylight{}
	daylight.AddProperty(ics.ComponentProperty(ics.PropertyDtstart), "19700308T020000")
	daylight.AddProperty(ics.ComponentProperty(ics.PropertyTzoffsetfrom), "-0700")
	daylight.AddProperty(ics.ComponentProperty(ics.PropertyTzoffsetto), "-0600")
	daylight.AddProperty(ics.ComponentProperty(ics.PropertyRrule), "FREQ=YEARLY;BYMONTH=3;BYDAY=2SU")
	daylight.AddProperty(ics.ComponentProperty(ics.PropertyTzname), "MDT")

	// Append the daylight component to the timezone's subcomponents
	tz.Components = append(tz.Components, daylight)
}

// addEvent creates a single VEVENT component from a game and adds it to the calendar.
func (g *Generator) addEvent(cal *ics.Calendar, game GameEvent, index int) error {
	// Parse the game datetime from an ISO string
	gameTime, err := time.Parse(time.RFC3339, game.DateStr)
	if err != nil {
		return fmt.Errorf("failed to parse game time: %w", err)
	}

	// Ensure time is in Mountain timezone
	gameTime = gameTime.In(g.location)

	// Create the event with a unique ID
	eventID := fmt.Sprintf("game-%s-%d@soccerschedule", game.GameID, index)
	event := cal.AddEvent(eventID)

	// Check if this is a special event (either team is in the special list)
	isSpecial := SpecialTeams[game.HomeTeam] || SpecialTeams[game.AwayTeam]

	// Set event summary (title)
	if isSpecial {
		event.SetSummary(fmt.Sprintf("Special Event: %s vs %s", game.HomeTeam, game.AwayTeam))
	} else {
		event.SetSummary(fmt.Sprintf("%s vs %s", game.HomeTeam, game.AwayTeam))
	}

	// Set the start time with timezone using AddProperty for TZID parameter
	// Format: DTSTART;TZID=America/Denver:20250115T190000
	dtStart := gameTime.Format("20060102T150405")
	event.AddProperty(ics.ComponentPropertyDtStart, dtStart, ics.WithTZID(TimezoneName))

	// Calculate and set end time (start + 45 minutes)
	endTime := gameTime.Add(DefaultDuration)
	dtEnd := endTime.Format("20060102T150405")
	event.AddProperty(ics.ComponentPropertyDtEnd, dtEnd, ics.WithTZID(TimezoneName))

	// Set location
	event.SetLocation(FixedLocation)

	// Set description
	var description string
	if isSpecial {
		description = fmt.Sprintf("Field %s\nSoccer game at Let's Play Soccer\n%s vs %s\nAhhh shit, here we go again...",
			game.Field, game.HomeTeam, game.AwayTeam)
	} else {
		description = fmt.Sprintf("Field %s\nSoccer game at Let's Play Soccer\n%s vs %s\nGLHF!",
			game.Field, game.HomeTeam, game.AwayTeam)
	}
	event.SetDescription(description)

	// Add a timestamp for when the event was created
	event.SetDtStampTime(time.Now().UTC())

	// Add alarm (40 minutes before)
	alarm := event.AddAlarm()
	alarm.SetAction(ics.ActionDisplay)
	alarm.SetTrigger(AlarmTrigger)
	alarm.AddProperty(ics.ComponentPropertyDescription, "Reminder: Soccer game starting soon")

	return nil
}

// GetFilename generates a suggested filename for the ICS file based on season and team.
// Format: {season}_{teamName}_{teamID}.ics
//
// Parameters:
//   - season: The season identifier (e.g., "2025")
//   - teamName: The team name (spaces will be replaced with underscores)
//   - teamID: The team ID
//
// Returns:
//   - string: The suggested filename
func GetFilename(season, teamName, teamID string) string {
	// Replace spaces with underscores in the team name
	safeName := strings.ReplaceAll(teamName, " ", "_")
	return fmt.Sprintf("%s_%s_%s.ics", season, safeName, teamID)
}
