package main

// Package main provides the AWS Lambda entrypoint for the soccer schedule scraper.
// This binary is designed to run behind API Gateway HTTP API (v2) and handles
// both 'fetch' and 'download' actions via query parameters.

import (
	"context"
	"log"

	"github.com/CraigDevJohnson/soccer_scraper/internal/app"
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

// handleRequest is the Lambda handler function that processes incoming
// API Gateway v2 HTTP events. It delegates to the app.Handler for all
// business logic.
//
// Parameters:
//   - ctx: Context with Lambda deadline and request ID
//   - request: The API Gateway v2 HTTP request event
//
// Returns:
//   - events.APIGatewayV2HTTPResponse: The HTTP response to return to the client
//   - error: Always nil - errors are returned in the response body
func handleRequest(ctx context.Context, request events.APIGatewayV2HTTPRequest) (events.APIGatewayV2HTTPResponse, error) {
	// Delegate to the handler for all processing
	response := handler.HandleRequest(ctx, request)
	return response, nil
}

// main starts the Lambda runtime and registers the handler.
func main() {
	lambda.Start(handleRequest)
}
