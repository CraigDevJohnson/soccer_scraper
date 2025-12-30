package main

// Package main provides the AWS Lambda entrypoint for the soccer schedule scraper.
// This binary is designed to run behind API Gateway HTTP API (v2) for fetch/download
// actions, and can also be triggered by EventBridge scheduled events for check-changes.

import (
	"context"
	"encoding/json"
	"log"
	"os"

	"github.com/CraigDevJohnson/soccer_scraper/internal/app"
	"github.com/CraigDevJohnson/soccer_scraper/internal/lps"
	"github.com/CraigDevJohnson/soccer_scraper/internal/notify"
	"github.com/CraigDevJohnson/soccer_scraper/internal/sns"
	"github.com/CraigDevJohnson/soccer_scraper/internal/storage"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
)

// handler is the initialized request handler, created once during cold start
// and reused across Lambda invocations for connection pooling efficiency.
var handler *app.Handler

// init runs during Lambda cold start to initialize the handler.
// This is done in init() rather than main() to ensure the handler
// is ready before any invocations are processed.
// During testing, this is skipped if SKIP_LAMBDA_INIT=true.
func init() {
	if os.Getenv("SKIP_LAMBDA_INIT") == "true" {
		return
	}
	initHandler()
}

// initHandler initializes the handler if it hasn't been initialized yet.
// This allows tests to skip initialization by setting handler to a mock value.
func initHandler() {
	if handler != nil {
		return // Already initialized (e.g., by tests)
	}
	var err error
	handler, err = app.NewHandler()
	if err != nil {
		// Log fatal error - Lambda will restart and retry
		log.Fatalf("Failed to initialize handler: %v", err)
	}
	log.Println("Handler initialized successfully")
}

// ScheduledEvent represents an EventBridge scheduled event payload.
// This is used to identify scheduled invocations vs. API Gateway requests.
type ScheduledEvent struct {
	// Source identifies EventBridge scheduled events.
	// Can be "aws.scheduler" (EventBridge Scheduler) or "aws.events" (EventBridge Rules).
	Source string `json:"source"`

	// DetailType describes the event type (e.g., "Scheduled Event")
	DetailType string `json:"detail-type"`

	// Detail contains custom event data (can include action override)
	Detail json.RawMessage `json:"detail"`
}

// CheckChangesResponse is returned when the Lambda is triggered for schedule checking.
type CheckChangesResponse struct {
	// Message describes the result of the operation
	Message string `json:"message"`

	// TeamsChecked is the number of teams that were checked
	TeamsChecked int `json:"teams_checked"`

	// TeamsWithChanges is the number of teams that had schedule changes
	TeamsWithChanges int `json:"teams_with_changes"`

	// Skipped indicates if the check was skipped (no teams to check)
	Skipped bool `json:"skipped,omitempty"`
}

// isScheduledEvent checks if the given raw event is an EventBridge scheduled event.
// It returns true if the event has source "aws.scheduler" or "aws.events" AND
// detail-type "Scheduled Event".
//
// Parameters:
//   - rawEvent: Raw JSON event data
//
// Returns:
//   - bool: true if this is a valid EventBridge scheduled event
//   - string: the source value if detected (for logging)
func isScheduledEvent(rawEvent json.RawMessage) (bool, string) {
	var scheduledEvent ScheduledEvent
	if err := json.Unmarshal(rawEvent, &scheduledEvent); err != nil {
		return false, ""
	}

	// Check if this is an EventBridge scheduled event by requiring both the
	// expected source and detail-type values. Using logical AND here avoids
	// misclassifying other event types (for example, API Gateway requests)
	// that might coincidentally include one of these fields.
	// Accept both "aws.scheduler" (EventBridge Scheduler) and "aws.events" (EventBridge Rules).
	isScheduler := (scheduledEvent.Source == "aws.scheduler" || scheduledEvent.Source == "aws.events") &&
		scheduledEvent.DetailType == "Scheduled Event"

	return isScheduler, scheduledEvent.Source
}

// handleRequest is the unified Lambda handler that routes between API Gateway
// and EventBridge scheduled events. It detects the event type and delegates it
// to the appropriate handler.
//
// Parameters:
//   - ctx: Context with Lambda deadline and request ID
//   - rawEvent: Raw JSON event (could be API Gateway or EventBridge)
//
// Returns:
//   - interface{}: Response (APIGatewayV2HTTPResponse or CheckChangesResponse)
//   - error: Any error during processing
func handleRequest(ctx context.Context, rawEvent json.RawMessage) (interface{}, error) {
	// Try to detect if this is an EventBridge scheduled event
	if isScheduler, source := isScheduledEvent(rawEvent); isScheduler {
		log.Printf("Detected EventBridge scheduled event (source=%q), running check-changes", source)
		return handleScheduledCheck(ctx)
	}

	// Debug log for events that might decode into ScheduledEvent but do not match
	var scheduledEvent ScheduledEvent
	if err := json.Unmarshal(rawEvent, &scheduledEvent); err == nil && scheduledEvent.Source != "" {
		// the exact EventBridge scheduled event signature. This helps diagnose
		// unexpected payloads without changing behavior for valid events.
		log.Printf("Decoded potential ScheduledEvent but did not match EventBridge criteria (source=%q, detailType=%q)", scheduledEvent.Source, scheduledEvent.DetailType)
	}

	// Otherwise, treat as API Gateway HTTP request
	var request events.APIGatewayV2HTTPRequest
	if err := json.Unmarshal(rawEvent, &request); err != nil {
		log.Printf("Failed to parse event as API Gateway request: %v", err)
		return events.APIGatewayV2HTTPResponse{
			StatusCode: 400,
			Body:       `{"error": "Invalid request format"}`,
		}, nil
	}

	// Delegate to the existing handler for all API Gateway processing
	response := handler.HandleRequest(ctx, request)
	return response, nil
}

// handleScheduledCheck processes an EventBridge scheduled event to check all
// teams for schedule changes. It first checks if there are any teams to check
// to minimize costs when no teams are subscribed.
//
// Parameters:
//   - ctx: Context with Lambda deadline and request ID
//   - event: The EventBridge scheduled event
//
// Returns:
//   - CheckChangesResponse: Result of the check operation
//   - error: Any error during processing
func handleScheduledCheck(ctx context.Context) (CheckChangesResponse, error) {
	log.Println("Starting scheduled schedule check")

	// Initialize the storage client to check for teams
	storageClient, err := storage.NewClient(ctx)
	if err != nil {
		log.Printf("Failed to create storage client: %v", err)
		return CheckChangesResponse{
			Message: "Failed to initialize storage client",
		}, err
	}

	// Check if there are any teams to check (cost optimization)
	hasTeams, err := storageClient.HasTeams(ctx)
	if err != nil {
		log.Printf("Failed to check for teams: %v", err)
		return CheckChangesResponse{
			Message: "Failed to check for teams in database",
		}, err
	}

	// If no teams, skip the check to save costs
	if !hasTeams {
		log.Println("No teams to check, skipping scheduled check")
		return CheckChangesResponse{
			Message:      "No teams subscribed, skipping check",
			TeamsChecked: 0,
			Skipped:      true,
		}, nil
	}

	// Initialize all required clients for the checker
	lpsClient, err := lps.NewClient()
	if err != nil {
		log.Printf("Failed to create LPS client: %v", err)
		return CheckChangesResponse{
			Message: "Failed to initialize LPS client",
		}, err
	}

	snsClient, err := sns.NewClient(ctx)
	if err != nil {
		log.Printf("Failed to create SNS client: %v", err)
		return CheckChangesResponse{
			Message: "Failed to initialize SNS client",
		}, err
	}

	// Create the checker and run the check
	checker := notify.NewChecker(lpsClient, storageClient, snsClient)
	changesCount, totalChecked, err := checker.CheckAllTeams(ctx)
	if err != nil {
		log.Printf("Error during scheduled check: %v", err)
		return CheckChangesResponse{
			Message:          "Error during schedule check",
			TeamsChecked:     totalChecked,
			TeamsWithChanges: changesCount,
		}, err
	}

	log.Printf("Scheduled check complete: %d/%d teams had changes", changesCount, totalChecked)
	return CheckChangesResponse{
		Message:          "Schedule check completed successfully",
		TeamsChecked:     totalChecked,
		TeamsWithChanges: changesCount,
	}, nil
}

// main starts the Lambda runtime and registers the handler.
func main() {
	lambda.Start(handleRequest)
}
