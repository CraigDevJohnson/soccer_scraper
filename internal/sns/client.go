package sns

// Package sns provides AWS SNS topic management for soccer schedule
// change notifications. It handles creating topics for teams, subscribing
// email addresses, and publishing schedule change notifications.

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"
)

// TopicPrefix is the prefix used for all soccer schedule notification topics.
// Topics are named: soccer-schedule-{teamID}
const TopicPrefix = "soccer-schedule-"

// Client handles SNS operations for schedule change notifications.
// It provides methods to create topics, subscribe emails, and publish notifications.
type Client struct {
	// snsClient is the underlying AWS SNS client.
	snsClient *sns.Client
}

// NewClient creates a new SNS client using the default AWS configuration.
// It loads credentials from environment variables, shared config, or IAM role.
//
// Returns an error if AWS configuration cannot be loaded.
func NewClient(ctx context.Context) (*Client, error) {
	// Load AWS configuration from environment/shared config
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create SNS client
	snsClient := sns.NewFromConfig(cfg)

	return &Client{
		snsClient: snsClient,
	}, nil
}

// GetOrCreateTopic ensures a topic exists for the given team ID and returns its ARN.
// Topic names follow the format: soccer-schedule-{teamID}
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - teamID: The 6-digit team ID to create a topic for
//
// Returns:
//   - string: The ARN of the created or existing topic
//   - error: Any error during topic creation
func (c *Client) GetOrCreateTopic(ctx context.Context, teamID string) (string, error) {
	// Build topic name
	topicName := TopicPrefix + teamID

	// CreateTopic is idempotent - it returns the existing topic ARN if it already exists
	log.Printf("Creating/getting SNS topic: %s", topicName)

	result, err := c.snsClient.CreateTopic(ctx, &sns.CreateTopicInput{
		Name: aws.String(topicName),
		Tags: []types.Tag{
			{
				Key:   aws.String("Application"),
				Value: aws.String("soccer-scraper"),
			},
			{
				Key:   aws.String("TeamID"),
				Value: aws.String(teamID),
			},
		},
	})
	if err != nil {
		return "", fmt.Errorf("failed to create/get SNS topic for team %s: %w", teamID, err)
	}

	log.Printf("Topic ARN: %s", *result.TopicArn)
	return *result.TopicArn, nil
}

// SubscribeEmail subscribes an email address to the team's notification topic.
// The subscriber will receive a confirmation email and must confirm before
// receiving notifications.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - topicArn: The ARN of the SNS topic to subscribe to
//   - email: The email address to subscribe
//
// Returns:
//   - string: The subscription ARN (pending confirmation until user confirms)
//   - error: Any error during subscription
func (c *Client) SubscribeEmail(ctx context.Context, topicArn, email string) (string, error) {
	log.Printf("Subscribing email %s to topic %s", email, topicArn)

	result, err := c.snsClient.Subscribe(ctx, &sns.SubscribeInput{
		TopicArn: aws.String(topicArn),
		Protocol: aws.String("email"),
		Endpoint: aws.String(email),
	})
	if err != nil {
		return "", fmt.Errorf("failed to subscribe email %s: %w", email, err)
	}

	subscriptionArn := "pending confirmation"
	if result.SubscriptionArn != nil {
		subscriptionArn = *result.SubscriptionArn
	}

	log.Printf("Subscription ARN: %s", subscriptionArn)
	return subscriptionArn, nil
}

// PublishScheduleChange publishes a notification about schedule changes to the topic.
// The message includes details about what changed (new games, removed games, updated games).
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - topicArn: The ARN of the SNS topic to publish to
//   - teamName: The name of the team for the notification subject
//   - message: The formatted message describing the schedule changes
//
// Returns:
//   - error: Any error during publishing
func (c *Client) PublishScheduleChange(ctx context.Context, topicArn, teamName, message string) error {
	subject := fmt.Sprintf("Schedule Update: %s", teamName)

	// Truncate subject if too long (SNS limit is 100 chars)
	if len(subject) > 100 {
		subject = subject[:97] + "..."
	}

	log.Printf("Publishing schedule change notification for %s", teamName)

	_, err := c.snsClient.Publish(ctx, &sns.PublishInput{
		TopicArn: aws.String(topicArn),
		Subject:  aws.String(subject),
		Message:  aws.String(message),
	})
	if err != nil {
		return fmt.Errorf("failed to publish schedule change: %w", err)
	}

	log.Printf("Schedule change notification published successfully")
	return nil
}

// ListSubscriptions returns all subscriptions for a given topic ARN.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - topicArn: The ARN of the SNS topic to list subscriptions for
//
// Returns:
//   - []Subscription: List of subscriptions with their details
//   - error: Any error during listing
func (c *Client) ListSubscriptions(ctx context.Context, topicArn string) ([]Subscription, error) {
	log.Printf("Listing subscriptions for topic %s", topicArn)

	var subscriptions []Subscription
	var nextToken *string

	for {
		result, err := c.snsClient.ListSubscriptionsByTopic(ctx, &sns.ListSubscriptionsByTopicInput{
			TopicArn:  aws.String(topicArn),
			NextToken: nextToken,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to list subscriptions: %w", err)
		}

		for _, sub := range result.Subscriptions {
			subscriptions = append(subscriptions, Subscription{
				SubscriptionArn: aws.ToString(sub.SubscriptionArn),
				Protocol:        aws.ToString(sub.Protocol),
				Endpoint:        aws.ToString(sub.Endpoint),
			})
		}

		if result.NextToken == nil {
			break
		}
		nextToken = result.NextToken
	}

	return subscriptions, nil
}

// Subscription represents an SNS subscription.
type Subscription struct {
	// SubscriptionArn is the ARN of the subscription.
	SubscriptionArn string

	// Protocol is the subscription protocol (e.g., "email").
	Protocol string

	// Endpoint is the subscription endpoint (e.g., email address).
	Endpoint string
}

// GetTopicArnFromTeamID extracts the team ID from a topic ARN.
// This is useful when processing topic ARNs to find the associated team.
//
// Parameters:
//   - topicArn: The full topic ARN
//
// Returns:
//   - string: The team ID extracted from the topic name, or empty if not found
func GetTopicArnFromTeamID(topicArn string) string {
	// Topic ARN format: arn:aws:sns:{region}:{account}:soccer-schedule-{teamID}
	// Extract the topic name (last part after the last :)
	parts := strings.Split(topicArn, ":")
	if len(parts) < 6 {
		return ""
	}
	topicName := parts[len(parts)-1]

	// Extract team ID from topic name
	if !strings.HasPrefix(topicName, TopicPrefix) {
		return ""
	}
	return strings.TrimPrefix(topicName, TopicPrefix)
}
