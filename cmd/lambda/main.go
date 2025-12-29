package main

// Package main provides the AWS Lambda entrypoint for the soccer schedule scraper.
// This binary is designed to run behind API Gateway HTTP API (v2) for fetch/download
// actions, and can also be triggered by EventBridge scheduled events for check-changes.

import (
	"context"
	"encoding/json"
	"log"

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
func init() {
	var err error
	handler, err = app.NewHandler()
	if err != nil {
		// Log fatal error - Lambda will restart and retry
		log.Fatalf("Failed to initialize handler: %v", err)
	}
	log.Println("Handler initialized successfully")
}

// ScheduledEvent represents an EventBridge scheduled event payload.
// This is used to identify scheduled invocations vs API Gateway requests.
type ScheduledEvent struct {
	// Source identifies EventBridge scheduled events (e.g., "aws.events")
	Source string `json:"source"`

	// DetailType describes the event type (e.g., "Scheduled Event")
	DetailType string `json:"detail-type"`

	// Detail contains custom event data (can include action override)
	Detail json.RawMessage `json:"detail"`
}

// ScheduledEventDetail contains optional configuration for scheduled events.
type ScheduledEventDetail struct {
	// Action specifies which action to run (defaults to "check-changes")
	Action string `json:"action"`
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

// handleRequest is the unified Lambda handler that routes between API Gateway
// and EventBridge scheduled events. It detects the event type and delegates
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
	var scheduledEvent ScheduledEvent
	if err := json.Unmarshal(rawEvent, &scheduledEvent); err == nil {
		// Check if this is an EventBridge scheduled event by requiring both the
		// expected source and detail-type values. Using logical AND here avoids
		// misclassifying other event types (for example, API Gateway requests)
		// that might coincidentally include one of these fields.
		if scheduledEvent.Source == "aws.events" && scheduledEvent.DetailType == "Scheduled Event" {
			log.Println("Detected EventBridge scheduled event, running check-changes")
			return handleScheduledCheck(ctx, scheduledEvent)
		}

		// Debug log for events that decode into ScheduledEvent but do not match
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
func handleScheduledCheck(ctx context.Context, event ScheduledEvent) (CheckChangesResponse, error) {
	log.Println("Starting scheduled schedule check")

	// Initialize storage client to check for teams
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
