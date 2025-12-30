package main

import (
	"encoding/json"
	"os"
	"testing"
)

// init sets environment variable to skip AWS service initialization during tests.
// In Go, init functions in test files typically execute before production code init
// functions in the same package, allowing us to configure the environment before
// the main package initialization runs. The SKIP_LAMBDA_INIT environment variable
// is checked by the main package's init function to avoid AWS service initialization.
func init() {
	os.Setenv("SKIP_LAMBDA_INIT", "true")
}

// TestIsScheduledEvent tests the isScheduledEvent function that determines
// whether an event should be routed to scheduled check vs API Gateway handling.
func TestIsScheduledEvent(t *testing.T) {
	tests := []struct {
		name           string
		json           string
		expectSchedule bool
		expectSource   string
		description    string
	}{
		{
			name:           "EventBridge Scheduler event",
			json:           `{"source":"aws.scheduler","detail-type":"Scheduled Event","detail":{}}`,
			expectSchedule: true,
			expectSource:   "aws.scheduler",
			description:    "EventBridge Scheduler events with source='aws.scheduler' should be detected",
		},
		{
			name:           "EventBridge Rules event",
			json:           `{"source":"aws.events","detail-type":"Scheduled Event","detail":{}}`,
			expectSchedule: true,
			expectSource:   "aws.events",
			description:    "EventBridge Rules events with source='aws.events' should be detected",
		},
		{
			name:           "Wrong source, correct detail-type",
			json:           `{"source":"aws.other","detail-type":"Scheduled Event","detail":{}}`,
			expectSchedule: false,
			expectSource:   "aws.other",
			description:    "Events with wrong source should not be detected as scheduled",
		},
		{
			name:           "Correct source (scheduler), wrong detail-type",
			json:           `{"source":"aws.scheduler","detail-type":"Other Event","detail":{}}`,
			expectSchedule: false,
			expectSource:   "aws.scheduler",
			description:    "Events with correct source but wrong detail-type should not be detected",
		},
		{
			name:           "Correct source (events), wrong detail-type",
			json:           `{"source":"aws.events","detail-type":"Other Event","detail":{}}`,
			expectSchedule: false,
			expectSource:   "aws.events",
			description:    "Events with correct source but wrong detail-type should not be detected",
		},
		{
			name:           "Empty source",
			json:           `{"source":"","detail-type":"Scheduled Event","detail":{}}`,
			expectSchedule: false,
			expectSource:   "",
			description:    "Events with empty source should not be detected as scheduled",
		},
		{
			name:           "Empty detail-type",
			json:           `{"source":"aws.scheduler","detail-type":"","detail":{}}`,
			expectSchedule: false,
			expectSource:   "aws.scheduler",
			description:    "Events with empty detail-type should not be detected as scheduled",
		},
		{
			name:           "API Gateway event (no source field)",
			json:           `{"requestContext":{"requestId":"test-id"},"rawQueryString":"action=fetch"}`,
			expectSchedule: false,
			expectSource:   "",
			description:    "API Gateway events should not be misclassified as scheduled events",
		},
		{
			name:           "Scheduler event with custom detail",
			json:           `{"source":"aws.scheduler","detail-type":"Scheduled Event","detail":{"custom":"data"}}`,
			expectSchedule: true,
			expectSource:   "aws.scheduler",
			description:    "Scheduler events with custom detail data should still be detected",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isScheduler, source := isScheduledEvent(json.RawMessage(tt.json))

			if isScheduler != tt.expectSchedule {
				t.Errorf("%s: Expected isScheduler=%v, got %v", tt.description, tt.expectSchedule, isScheduler)
			}

			if source != tt.expectSource {
				t.Errorf("%s: Expected source=%q, got %q", tt.description, tt.expectSource, source)
			}
		})
	}
}

// TestScheduledEventParsing tests that ScheduledEvent struct correctly parses JSON.
func TestScheduledEventParsing(t *testing.T) {
	tests := []struct {
		name       string
		json       string
		expectErr  bool
		wantSource string
		wantDetail string
	}{
		{
			name:       "EventBridge Scheduler payload",
			json:       `{"source":"aws.scheduler","detail-type":"Scheduled Event","detail":{}}`,
			expectErr:  false,
			wantSource: "aws.scheduler",
			wantDetail: "Scheduled Event",
		},
		{
			name:       "EventBridge Rules payload",
			json:       `{"source":"aws.events","detail-type":"Scheduled Event","detail":{}}`,
			expectErr:  false,
			wantSource: "aws.events",
			wantDetail: "Scheduled Event",
		},
		{
			name:       "Payload with custom detail",
			json:       `{"source":"aws.scheduler","detail-type":"Scheduled Event","detail":{"custom":"data"}}`,
			expectErr:  false,
			wantSource: "aws.scheduler",
			wantDetail: "Scheduled Event",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var event ScheduledEvent
			err := json.Unmarshal([]byte(tt.json), &event)

			if tt.expectErr && err == nil {
				t.Errorf("Expected error but got none")
			}
			if !tt.expectErr && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			if event.Source != tt.wantSource {
				t.Errorf("Expected source %q, got %q", tt.wantSource, event.Source)
			}
			if event.DetailType != tt.wantDetail {
				t.Errorf("Expected detail-type %q, got %q", tt.wantDetail, event.DetailType)
			}
		})
	}
}

// TestEventDetectionLogic tests the core boolean logic for event detection
// by calling the actual isScheduledEvent function with constructed JSON payloads.
func TestEventDetectionLogic(t *testing.T) {
	tests := []struct {
		source         string
		detailType     string
		expectSchedule bool
		description    string
	}{
		{
			source:         "aws.scheduler",
			detailType:     "Scheduled Event",
			expectSchedule: true,
			description:    "aws.scheduler with correct detail-type should be detected as scheduled",
		},
		{
			source:         "aws.events",
			detailType:     "Scheduled Event",
			expectSchedule: true,
			description:    "aws.events with correct detail-type should be detected as scheduled",
		},
		{
			source:         "aws.other",
			detailType:     "Scheduled Event",
			expectSchedule: false,
			description:    "Unknown source should not be detected as scheduled",
		},
		{
			source:         "aws.scheduler",
			detailType:     "Other Event",
			expectSchedule: false,
			description:    "Correct source with wrong detail-type should not be detected as scheduled",
		},
		{
			source:         "aws.events",
			detailType:     "Other Event",
			expectSchedule: false,
			description:    "Correct source with wrong detail-type should not be detected as scheduled",
		},
		{
			source:         "",
			detailType:     "Scheduled Event",
			expectSchedule: false,
			description:    "Empty source should not be detected as scheduled",
		},
		{
			source:         "aws.scheduler",
			detailType:     "",
			expectSchedule: false,
			description:    "Empty detail-type should not be detected as scheduled",
		},
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			// Construct a JSON payload with the test values
			payload := map[string]string{
				"source":      tt.source,
				"detail-type": tt.detailType,
			}
			jsonBytes, err := json.Marshal(payload)
			if err != nil {
				t.Fatalf("Failed to marshal test payload: %v", err)
			}

			// Call the actual isScheduledEvent function
			isScheduler, _ := isScheduledEvent(jsonBytes)

			if isScheduler != tt.expectSchedule {
				t.Errorf("Expected isScheduler=%v, got %v for source=%q, detailType=%q",
					tt.expectSchedule, isScheduler, tt.source, tt.detailType)
			}
		})
	}
}
