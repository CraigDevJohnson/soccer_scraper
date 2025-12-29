package app

// Package app provides the core application handler logic for the soccer
// schedule scraper. It routes incoming requests to the appropriate action
// (fetch, download, or subscribe) and formats responses.

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/mail"

	"github.com/CraigDevJohnson/soccer_scraper/internal/calendar"
	"github.com/CraigDevJohnson/soccer_scraper/internal/lps"
	"github.com/CraigDevJohnson/soccer_scraper/internal/sns"
	"github.com/CraigDevJohnson/soccer_scraper/internal/storage"
	"github.com/CraigDevJohnson/soccer_scraper/internal/validate"
	"github.com/aws/aws-lambda-go/events"
)

// Handler processes incoming API Gateway requests and routes them to the
// appropriate action handler. It maintains references to the LPS client
// and calendar generator for reuse across invocations.
type Handler struct {
	// lpsClient is the HTTP client for LPS API requests.
	lpsClient *lps.Client

	// icsGenerator creates ICS calendar files from game data.
	icsGenerator *calendar.Generator
}

// NewHandler creates a new Handler with an initialized LPS client and ICS generator.
// This should be called once during Lambda cold start and reused across invocations.
//
// Returns an error if the LPS client or ICS generator fails to initialize
// (typically due to timezone loading issues, which should not happen with
// embedded tzdata).
func NewHandler() (*Handler, error) {
	// Initialize LPS API client
	lpsClient, err := lps.NewClient()
	if err != nil {
		return nil, fmt.Errorf("failed to create LPS client: %w", err)
	}

	// Initialize ICS generator
	icsGen, err := calendar.NewGenerator()
	if err != nil {
		return nil, fmt.Errorf("failed to create ICS generator: %w", err)
	}

	return &Handler{
		lpsClient:    lpsClient,
		icsGenerator: icsGen,
	}, nil
}

// HandleRequest processes an API Gateway v2 HTTP request and returns the appropriate response.
// It routes based on the 'action' query parameter:
//   - 'fetch' (default): Retrieve game schedules for specified team IDs
//   - 'download': Generate an ICS calendar file from provided games
//   - 'subscribe': Subscribe an email address to schedule change notifications
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - request: The API Gateway v2 HTTP request event
//
// Returns:
//   - events.APIGatewayV2HTTPResponse: The HTTP response with appropriate status, headers, and body
func (h *Handler) HandleRequest(ctx context.Context, request events.APIGatewayV2HTTPRequest) events.APIGatewayV2HTTPResponse {
	// Log version for debugging deployed versions
	log.Printf("Soccer Schedule API Version: %s", Version)

	// Get action from query parameters, default to 'fetch'
	action := request.QueryStringParameters["action"]
	if action == "" {
		action = ActionFetch
	}

	// Route to the appropriate handler
	switch action {
	case ActionFetch:
		return h.handleFetch(ctx, request)
	case ActionDownload:
		return h.handleDownload(ctx, request)
	case ActionSubscribe:
		return h.handleSubscribe(ctx, request)
	default:
		return h.errorResponse(400, "Invalid action", "ValidationError", nil, nil, nil)
	}
}

// handleFetch processes the 'fetch' action to retrieve game schedules for team IDs.
// It validates team IDs, fetches schedules concurrently, and returns partial results
// if some teams fail while others succeed.
func (h *Handler) handleFetch(ctx context.Context, request events.APIGatewayV2HTTPRequest) events.APIGatewayV2HTTPResponse {
	// Get team_ids from query parameters
	teamIDsParam := request.QueryStringParameters["team_ids"]
	if teamIDsParam == "" {
		return h.errorResponse(400,
			"Team IDs are required. Please provide at least one valid 6-digit team ID.",
			"ValidationError", nil, nil, nil)
	}

	// Parse and validate team IDs
	validTeamIDs, invalidTeamIDs := validate.ParseTeamIDsCSV(teamIDsParam)

	// Collect validation error messages for response
	var validationErrors []string
	for _, inv := range invalidTeamIDs {
		validationErrors = append(validationErrors, inv.Reason)
	}

	// If no valid team IDs, return the error with details
	if len(validTeamIDs) == 0 {
		return h.validationErrorResponse(invalidTeamIDs, validationErrors)
	}

	// Fetch schedules for all valid team IDs concurrently
	games, failedTeams := h.lpsClient.FetchMultipleTeams(ctx, validTeamIDs)

	// Build response with games and any error details
	response := FetchResponse{
		Games:            games,
		ProcessedTeamIDs: validTeamIDs,
	}

	// Include failed teams if any
	if len(failedTeams) > 0 {
		response.FailedTeams = failedTeams
	}

	// Include invalid team IDs if any
	if len(invalidTeamIDs) > 0 {
		response.InvalidTeamIDs = invalidTeamIDs
	}

	// Return a successful response (even with partial failures)
	return h.jsonResponse(200, response)
}

// handleDownload processes the 'download' action to generate an ICS calendar file.
// It expects a JSON body with a 'games' array and returns the ICS content.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control (currently unused but included for consistency and future extensibility)
//   - request: The API Gateway v2 HTTP request event
func (h *Handler) handleDownload(ctx context.Context, request events.APIGatewayV2HTTPRequest) events.APIGatewayV2HTTPResponse {
	// Get the request body
	body := request.Body

	// Handle base64-encoded bodies (API Gateway binary handling)
	if request.IsBase64Encoded && body != "" {
		decoded, err := base64.StdEncoding.DecodeString(body)
		if err != nil {
			return h.errorResponse(400, "Invalid base64 encoding in request body", "ValidationError", nil, nil, nil)
		}
		body = string(decoded)
	}

	// Check for empty body
	if body == "" {
		return h.errorResponse(400, "No games provided for calendar", "ValidationError", nil, nil, nil)
	}

	// Parse the request body
	var downloadReq DownloadRequest
	if err := json.Unmarshal([]byte(body), &downloadReq); err != nil {
		return h.errorResponse(400, "Invalid JSON in request body", "ValidationError", nil, nil, nil)
	}

	// Validate that games were provided
	if len(downloadReq.Games) == 0 {
		return h.errorResponse(400, "No games provided for calendar", "ValidationError", nil, nil, nil)
	}

	// Convert app.Game to calendar.GameEvent for ICS generation using the helper
	gameEvents := calendar.FromAppGames(downloadReq.Games)

	// Generate the ICS calendar
	icsContent, err := h.icsGenerator.GenerateICS(gameEvents)
	if err != nil {
		log.Printf("Error generating calendar: %v", err)
		return h.errorResponse(500,
			fmt.Sprintf("Failed to generate calendar: %v", err),
			"RuntimeError", nil, nil, nil)
	}

	// Determine filename from games (use first game's season if available)
	filename := "soccer_schedule.ics"
	if len(downloadReq.Games) > 0 {
		firstGame := downloadReq.Games[0]
		if firstGame.Season != "" {
			filename = fmt.Sprintf("soccer_schedule_%s.ics", firstGame.Season)
		}
	}

	// Return the ICS content as text/calendar
	return events.APIGatewayV2HTTPResponse{
		StatusCode: 200,
		Headers: map[string]string{
			"Content-Type":                "text/calendar; charset=utf-8",
			"Content-Disposition":         fmt.Sprintf("attachment; filename=\"%s\"", filename),
			"Access-Control-Allow-Origin": "*",
		},
		Body: icsContent,
	}
}

// jsonResponse creates a JSON response with the given status code and body.
func (h *Handler) jsonResponse(statusCode int, body interface{}) events.APIGatewayV2HTTPResponse {
	jsonBody, err := json.Marshal(body)
	if err != nil {
		log.Printf("Error marshaling response: %v", err)
		return h.errorResponse(500, "Internal server error", "RuntimeError", nil, nil, nil)
	}

	headers := map[string]string{
		"Content-Type":                "application/json",
		"Access-Control-Allow-Origin": "*",
	}

	return events.APIGatewayV2HTTPResponse{
		StatusCode: statusCode,
		Headers:    headers,
		Body:       string(jsonBody),
	}
}

// errorResponse creates a JSON error response with the given details.
func (h *Handler) errorResponse(statusCode int, message, errorType string,
	processedTeamIDs []string, failedTeams []FailedTeam, invalidTeamIDs []InvalidTeamID) events.APIGatewayV2HTTPResponse {

	errResp := ErrorResponse{
		Error:            message,
		ErrorType:        errorType,
		ProcessedTeamIDs: processedTeamIDs,
		FailedTeams:      failedTeams,
		InvalidTeamIDs:   invalidTeamIDs,
	}

	return h.jsonResponse(statusCode, errResp)
}

// validationErrorResponse creates a 400 response for validation failures.
func (h *Handler) validationErrorResponse(invalidTeamIDs []InvalidTeamID, validationErrors []string) events.APIGatewayV2HTTPResponse {
	errResp := ErrorResponse{
		Error:            "No valid team IDs provided",
		ErrorType:        "ValidationError",
		InvalidTeamIDs:   invalidTeamIDs,
		ValidationErrors: validationErrors,
	}

	return h.jsonResponse(400, errResp)
}

// handleSubscribe processes the 'subscribe' action to register email notifications for a team.
// It validates the team ID and email, fetches the current schedule, creates an SNS topic,
// subscribes the email, and stores the schedule in DynamoDB for future comparison.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - request: The API Gateway v2 HTTP request event
//
// Returns:
//   - events.APIGatewayV2HTTPResponse: The HTTP response with subscription details or error
func (h *Handler) handleSubscribe(ctx context.Context, request events.APIGatewayV2HTTPRequest) events.APIGatewayV2HTTPResponse {
	// Get team_id from query parameters
	teamID := request.QueryStringParameters["team_id"]
	if teamID == "" {
		return h.errorResponse(400,
			"Team ID is required. Please provide a valid 6-digit team ID.",
			"ValidationError", nil, nil, nil)
	}

	// Validate team ID
	if err := validate.TeamID(teamID); err != nil {
		return h.errorResponse(400,
			fmt.Sprintf("Invalid team ID '%s': %v", teamID, err),
			"ValidationError", nil, nil, nil)
	}

	// Get email from query parameters
	email := request.QueryStringParameters["email"]
	if email == "" {
		return h.errorResponse(400,
			"Email address is required.",
			"ValidationError", nil, nil, nil)
	}

	// Validate email using net/mail.ParseAddress (RFC 5322 compliant)
	parsedEmail, err := mail.ParseAddress(email)
	if err != nil {
		return h.errorResponse(400,
			fmt.Sprintf("Invalid email format '%s': %v", email, err),
			"ValidationError", nil, nil, nil)
	}
	// Use the parsed address (handles cases like "Name <email@example.com>")
	email = parsedEmail.Address

	log.Printf("Processing subscription request for team %s, email %s", teamID, email)

	// Fetch current schedule from LPS API
	result := h.lpsClient.FetchTeamSchedule(ctx, teamID)
	if result.Error != nil {
		log.Printf("Failed to fetch schedule for team %s: %v", teamID, result.Error)
		return h.errorResponse(500,
			fmt.Sprintf("Failed to fetch schedule: %v", result.Error),
			"RuntimeError", nil, nil, nil)
	}

	log.Printf("Fetched %d games for team %s (%s)", len(result.Games), teamID, result.TeamName)

	// Create SNS client
	snsClient, err := sns.NewClient(ctx)
	if err != nil {
		log.Printf("Failed to create SNS client: %v", err)
		return h.errorResponse(500,
			fmt.Sprintf("Failed to initialize notification service: %v", err),
			"RuntimeError", nil, nil, nil)
	}

	// Create or get the SNS topic for this team
	topicArn, err := snsClient.GetOrCreateTopic(ctx, teamID)
	if err != nil {
		log.Printf("Failed to create/get SNS topic for team %s: %v", teamID, err)
		return h.errorResponse(500,
			fmt.Sprintf("Failed to create notification topic: %v", err),
			"RuntimeError", nil, nil, nil)
	}

	// Subscribe email to the topic
	subArn, err := snsClient.SubscribeEmail(ctx, topicArn, email)
	if err != nil {
		log.Printf("Failed to subscribe email %s to topic %s: %v", email, topicArn, err)
		return h.errorResponse(500,
			fmt.Sprintf("Failed to subscribe email: %v", err),
			"RuntimeError", nil, nil, nil)
	}

	// Create storage client
	storageClient, err := storage.NewClient(ctx)
	if err != nil {
		log.Printf("Failed to create storage client: %v", err)
		return h.errorResponse(500,
			fmt.Sprintf("Failed to initialize storage: %v", err),
			"RuntimeError", nil, nil, nil)
	}

	// Convert games to stored format using the shared helper functions
	typesGames := lps.ConvertParsedGamesToTypesGames(result.Games, teamID, result.TeamName, result.Season)
	games := storage.ConvertGamesToStoredGames(typesGames)

	// Save schedule for future comparison
	schedule := &storage.StoredSchedule{
		TeamID:   teamID,
		TeamName: result.TeamName,
		Season:   result.Season,
		Games:    games,
		TopicArn: topicArn,
	}
	err = storageClient.SaveSchedule(ctx, schedule)
	if err != nil {
		log.Printf("Failed to save schedule for team %s: %v", teamID, err)
		return h.errorResponse(500,
			fmt.Sprintf("Failed to save schedule: %v", err),
			"RuntimeError", nil, nil, nil)
	}

	log.Printf("Subscription successful for team %s, email %s", teamID, email)

	// Return success response
	response := SubscribeResponse{
		Success:         true,
		Message:         "Subscription successful. Please check your email to confirm the subscription.",
		TeamID:          teamID,
		TeamName:        result.TeamName,
		Email:           email,
		SubscriptionArn: subArn,
		TopicArn:        topicArn,
	}

	return h.jsonResponse(200, response)
}
