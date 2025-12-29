package notify

// Package notify provides schedule comparison and change detection logic.
// It compares current schedules with stored ones to detect additions,
// removals, and updates to games, then formats notifications for users.

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/CraigDevJohnson/soccer_scraper/internal/lps"
	"github.com/CraigDevJohnson/soccer_scraper/internal/sns"
	"github.com/CraigDevJohnson/soccer_scraper/internal/storage"
	"github.com/CraigDevJohnson/soccer_scraper/internal/types"
	"golang.org/x/sync/errgroup"
)

// ScheduleChange represents a detected change in a team's schedule.
type ScheduleChange struct {
	// TeamID is the team ID that had schedule changes.
	TeamID string

	// TeamName is the team's display name.
	TeamName string

	// AddedGames are games that are new in the current schedule.
	AddedGames []storage.StoredGame

	// RemovedGames are games that were in the old schedule but not the new one.
	RemovedGames []storage.StoredGame

	// UpdatedGames are games that exist in both but have different details.
	// Each entry is a pair: [oldGame, newGame]
	UpdatedGames []GameUpdate
}

// GameUpdate represents a game that changed between schedule checks.
type GameUpdate struct {
	// OldGame is the previous version of the game.
	OldGame storage.StoredGame

	// NewGame is the current version of the game.
	NewGame storage.StoredGame

	// ChangedFields describes what changed (e.g., "time", "field").
	ChangedFields []string
}

// HasChanges returns true if there are any schedule changes.
func (c *ScheduleChange) HasChanges() bool {
	return len(c.AddedGames) > 0 || len(c.RemovedGames) > 0 || len(c.UpdatedGames) > 0
}

// Checker handles schedule change detection and notifications.
// It coordinates between the LPS client, storage, and SNS to detect
// and notify users of schedule changes.
type Checker struct {
	// lpsClient fetches current schedules from the LPS API.
	lpsClient *lps.Client

	// storageClient manages stored schedules in DynamoDB.
	storageClient *storage.Client

	// snsClient publishes notifications to SNS topics.
	snsClient *sns.Client
}

// NewChecker creates a new schedule change checker with all required clients.
//
// Parameters:
//   - lpsClient: Client for fetching current schedules
//   - storageClient: Client for accessing stored schedules
//   - snsClient: Client for publishing notifications
//
// Returns:
//   - *Checker: The initialized checker
func NewChecker(lpsClient *lps.Client, storageClient *storage.Client, snsClient *sns.Client) *Checker {
	return &Checker{
		lpsClient:     lpsClient,
		storageClient: storageClient,
		snsClient:     snsClient,
	}
}

// NewCheckerWithClients initializes all required clients and returns a new Checker.
// This helper simplifies checker setup in both Lambda and CLI environments.
//
// Parameters:
//   - ctx: Context for client initialization
//
// Returns:
//   - *Checker: The initialized checker
//   - error: Any error during client initialization
func NewCheckerWithClients(ctx context.Context) (*Checker, error) {
	lpsClient, err := lps.NewClient()
	if err != nil {
		return nil, fmt.Errorf("failed to create LPS client: %w", err)
	}

	storageClient, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage client: %w", err)
	}

	snsClient, err := sns.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create SNS client: %w", err)
	}

	return NewChecker(lpsClient, storageClient, snsClient), nil
}

// CompareSchedules compares a current schedule with a stored one to detect changes.
//
// Parameters:
//   - stored: The previously stored schedule
//   - current: The current games from the API
//   - teamName: The team name for the change report
//
// Returns:
//   - *ScheduleChange: The detected changes (empty if no changes)
func CompareSchedules(stored *storage.StoredSchedule, current []types.Game, teamName string) *ScheduleChange {
	change := &ScheduleChange{
		TeamID:   stored.TeamID,
		TeamName: teamName,
	}

	// Build maps for efficient comparison
	// Key is GameID for matching games
	storedMap := make(map[string]storage.StoredGame)
	for _, g := range stored.Games {
		storedMap[g.GameID] = g
	}

	currentMap := make(map[string]storage.StoredGame)
	storedGames := storage.ConvertGamesToStoredGames(current)
	for _, sg := range storedGames {
		currentMap[sg.GameID] = sg
	}

	// Find added and updated games
	for gameID, currentGame := range currentMap {
		storedGame, exists := storedMap[gameID]
		if !exists {
			// Game is new
			change.AddedGames = append(change.AddedGames, currentGame)
		} else {
			// Game exists - check for updates
			changedFields := compareGames(storedGame, currentGame)
			if len(changedFields) > 0 {
				change.UpdatedGames = append(change.UpdatedGames, GameUpdate{
					OldGame:       storedGame,
					NewGame:       currentGame,
					ChangedFields: changedFields,
				})
			}
		}
	}

	// Find removed games
	for gameID, storedGame := range storedMap {
		if _, exists := currentMap[gameID]; !exists {
			change.RemovedGames = append(change.RemovedGames, storedGame)
		}
	}

	// Sort all slices by GameID for consistent, predictable ordering in notifications
	// GameID typically reflects chronological order (earlier games have lower IDs)
	sort.Slice(change.AddedGames, func(i, j int) bool {
		return change.AddedGames[i].GameID < change.AddedGames[j].GameID
	})
	sort.Slice(change.RemovedGames, func(i, j int) bool {
		return change.RemovedGames[i].GameID < change.RemovedGames[j].GameID
	})
	sort.Slice(change.UpdatedGames, func(i, j int) bool {
		return change.UpdatedGames[i].NewGame.GameID < change.UpdatedGames[j].NewGame.GameID
	})

	return change
}

// compareGames checks if two games differ and returns the list of changed fields.
func compareGames(old, new storage.StoredGame) []string {
	var changed []string

	if old.DateStr != new.DateStr {
		changed = append(changed, "time")
	}
	if old.Field != new.Field {
		changed = append(changed, "field")
	}
	if old.HomeTeam != new.HomeTeam {
		changed = append(changed, "home team")
	}
	if old.AwayTeam != new.AwayTeam {
		changed = append(changed, "away team")
	}

	return changed
}

// FormatChangeNotification creates a human-readable message for schedule changes.
//
// Parameters:
//   - change: The detected schedule changes
//
// Returns:
//   - string: A formatted notification message
func FormatChangeNotification(change *ScheduleChange) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("Schedule Update for %s\n", change.TeamName))
	sb.WriteString(strings.Repeat("=", 40))
	sb.WriteString("\n\n")

	// Added games
	if len(change.AddedGames) > 0 {
		sb.WriteString(fmt.Sprintf("🆕 NEW GAMES (%d):\n", len(change.AddedGames)))
		for _, g := range change.AddedGames {
			sb.WriteString(formatGameLine(g))
		}
		sb.WriteString("\n")
	}

	// Removed games
	if len(change.RemovedGames) > 0 {
		sb.WriteString(fmt.Sprintf("❌ CANCELLED/REMOVED GAMES (%d):\n", len(change.RemovedGames)))
		for _, g := range change.RemovedGames {
			sb.WriteString(formatGameLine(g))
		}
		sb.WriteString("\n")
	}

	// Updated games
	if len(change.UpdatedGames) > 0 {
		sb.WriteString(fmt.Sprintf("📝 UPDATED GAMES (%d):\n", len(change.UpdatedGames)))
		for _, u := range change.UpdatedGames {
			sb.WriteString(fmt.Sprintf("  Game: %s vs %s\n", u.NewGame.HomeTeam, u.NewGame.AwayTeam))
			sb.WriteString(fmt.Sprintf("  Changed: %s\n", strings.Join(u.ChangedFields, ", ")))
			sb.WriteString(fmt.Sprintf("    OLD: %s at Field %s\n", formatDateTime(u.OldGame.DateStr), u.OldGame.Field))
			sb.WriteString(fmt.Sprintf("    NEW: %s at Field %s\n", formatDateTime(u.NewGame.DateStr), u.NewGame.Field))
			sb.WriteString("\n")
		}
	}

	sb.WriteString("---\n")
	sb.WriteString("This notification was sent by Soccer Schedule Tracker.\n")
	sb.WriteString("To unsubscribe, click the link in the original confirmation email.\n")

	return sb.String()
}

// formatGameLine formats a single game for the notification message.
func formatGameLine(g storage.StoredGame) string {
	return fmt.Sprintf("  • %s vs %s - %s at Field %s\n",
		g.HomeTeam, g.AwayTeam, formatDateTime(g.DateStr), g.Field)
}

// formatDateTime formats an ISO datetime string for display in Mountain Time.
// The output includes the timezone abbreviation (MST or MDT) for clarity.
// If MountainTime is not initialized, it falls back to UTC formatting.
func formatDateTime(isoDate string) string {
	t, err := time.Parse(time.RFC3339, isoDate)
	if err != nil {
		return isoDate // Return as-is if parsing fails
	}

	// Use the shared Mountain Time location from the LPS package if available
	if lps.MountainTime != nil {
		return t.In(lps.MountainTime).Format("Mon Jan 2 at 3:04 PM MST")
	}

	// Fallback to UTC if MountainTime is not initialized (should not happen in normal operation)
	return t.UTC().Format("Mon Jan 2 at 3:04 PM (UTC)")
}

// CheckAndNotify checks a single team's schedule for changes and sends notifications.
// This is the main entry point for the scheduled checker.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - teamID: The team ID to check
//
// Returns:
//   - bool: True if changes were detected and notification sent
//   - error: Any error during checking or notification
func (c *Checker) CheckAndNotify(ctx context.Context, teamID string) (bool, error) {
	log.Printf("Checking schedule for team %s", teamID)

	// Get stored schedule
	stored, err := c.storageClient.GetSchedule(ctx, teamID)
	if err != nil {
		return false, fmt.Errorf("failed to get stored schedule: %w", err)
	}

	// If no stored schedule, nothing to compare
	if stored == nil {
		log.Printf("No stored schedule for team %s, skipping comparison", teamID)
		return false, nil
	}

	// Fetch current schedule from API
	result := c.lpsClient.FetchTeamSchedule(ctx, teamID)
	if result.Error != nil {
		return false, fmt.Errorf("failed to fetch current schedule: %w", result.Error)
	}

	// Convert to types.Game for comparison using the helper function
	currentGames := lps.ConvertParsedGamesToTypesGames(result.Games, result.TeamID, result.TeamName, result.Season)

	// Compare schedules
	change := CompareSchedules(stored, currentGames, result.TeamName)

	// If no changes, update the last checked time and return
	if !change.HasChanges() {
		log.Printf("No schedule changes detected for team %s", teamID)
		// Update stored schedule with new last checked time
		stored.LastChecked = time.Now()
		stored.Games = storage.ConvertGamesToStoredGames(currentGames)
		if err := c.storageClient.SaveSchedule(ctx, stored); err != nil {
			log.Printf("Warning: failed to update stored schedule for team %s: %v", teamID, err)
		}
		return false, nil
	}

	// Changes detected - send notification
	log.Printf("Schedule changes detected for team %s: %d added, %d removed, %d updated",
		teamID, len(change.AddedGames), len(change.RemovedGames), len(change.UpdatedGames))

	// Check if we have a topic ARN to notify
	if stored.TopicArn == "" {
		log.Printf("No topic ARN for team %s, skipping notification", teamID)
	} else {
		// Format and send notification
		message := FormatChangeNotification(change)
		err = c.snsClient.PublishScheduleChange(ctx, stored.TopicArn, change.TeamName, message)
		if err != nil {
			log.Printf("Warning: failed to send notification: %v", err)
			// Don't fail the whole operation - still update the stored schedule
		} else {
			log.Printf("Notification sent for team %s", teamID)
		}
	}

	// Update the stored schedule with current data
	stored.Games = storage.ConvertGamesToStoredGames(currentGames)
	stored.TeamName = result.TeamName
	stored.Season = result.Season
	stored.LastChecked = time.Now()
	err = c.storageClient.SaveSchedule(ctx, stored)
	if err != nil {
		return true, fmt.Errorf("changes detected but failed to update stored schedule: %w", err)
	}

	return true, nil
}

// MaxConcurrentChecks is the maximum number of concurrent team checks.
// This prevents overwhelming the LPS API and respects Lambda CPU constraints.
const MaxConcurrentChecks = 8

// CheckAllTeams checks all stored teams for schedule changes concurrently.
// This is the main entry point for a scheduled Lambda invocation.
// Uses bounded concurrency to improve performance while preventing API overload.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//
// Returns:
//   - int: Number of teams with changes
//   - int: Total teams checked
//   - error: Any error during processing
func (c *Checker) CheckAllTeams(ctx context.Context) (int, int, error) {
	log.Printf("Starting scheduled check for all teams")

	// Get all stored schedules
	schedules, err := c.storageClient.ListAllSchedules(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to list schedules: %w", err)
	}

	if len(schedules) == 0 {
		log.Printf("No teams to check")
		return 0, 0, nil
	}

	log.Printf("Checking %d teams concurrently (max %d at a time)", len(schedules), MaxConcurrentChecks)

	// Use mutex to safely collect results from concurrent goroutines
	var mu sync.Mutex
	changesCount := 0
	errorsCount := 0

	// Create errgroup with bounded concurrency
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(MaxConcurrentChecks)

	for _, schedule := range schedules {
		// Capture schedule for goroutine closure
		schedule := schedule

		g.Go(func() error {
			hasChanges, err := c.CheckAndNotify(ctx, schedule.TeamID)

			// Lock to safely update shared counters
			mu.Lock()
			defer mu.Unlock()

			if err != nil {
				log.Printf("Error checking team %s: %v", schedule.TeamID, err)
				errorsCount++
				// Return nil to allow other checks to continue
				return nil
			}
			if hasChanges {
				changesCount++
			}
			return nil
		})
	}

	// Wait for all checks to complete
	_ = g.Wait()

	log.Printf("Scheduled check complete: %d/%d teams had changes (%d errors)", changesCount, len(schedules), errorsCount)
	return changesCount, len(schedules), nil
}
