package storage

// Package storage provides DynamoDB-based persistence for soccer schedules.
// It stores schedules with a TTL for automatic cleanup after the season ends.
// It also supports comparing current schedules with stored ones to detect changes.

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	sharedtypes "github.com/CraigDevJohnson/soccer_scraper/internal/types"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Configuration constants for DynamoDB storage.
const (
	// TableName is the DynamoDB table name for storing schedules.
	TableName = "soccer-schedules"

	// DefaultTTLDays is the default number of days to retain schedule data.
	// Set to 90 days to cover an 8-week season with buffer.
	DefaultTTLDays = 90

	// DefaultTTLDuration is the default duration to retain schedule data.
	// Calculated from DefaultTTLDays for cleaner time calculations.
	DefaultTTLDuration = DefaultTTLDays * 24 * time.Hour
)

// Client handles DynamoDB operations for schedule storage.
// It provides methods to store, retrieve, and compare schedules.
type Client struct {
	// dynamoClient is the underlying AWS DynamoDB client.
	dynamoClient *dynamodb.Client

	// tableName is the DynamoDB table name (can be overridden for testing).
	tableName string
}

// NewClient creates a new DynamoDB storage client using the default AWS configuration.
// It loads credentials from environment variables, shared config, or IAM role.
// The client ensures the DynamoDB table exists on initialization, creating it if necessary.
//
// Returns an error if AWS configuration cannot be loaded or table creation fails.
func NewClient(ctx context.Context) (*Client, error) {
	// Load AWS configuration from environment/shared config
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create DynamoDB client
	dynamoClient := dynamodb.NewFromConfig(cfg)

	client := &Client{
		dynamoClient: dynamoClient,
		tableName:    TableName,
	}

	// Ensure the table exists, creating it if necessary
	if err := client.ensureTableExists(ctx); err != nil {
		return nil, fmt.Errorf("failed to ensure table exists: %w", err)
	}

	return client, nil
}

// ensureTableExists checks if the DynamoDB table exists and creates it if it doesn't.
// This method is idempotent - it's safe to call multiple times.
// The table is created with:
//   - Partition key: team_id (String)
//   - Pay-per-request billing mode (no provisioned capacity needed)
//   - TTL enabled on the 'ttl' attribute for automatic data cleanup
//   - Tags for identifying the application
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//
// Returns:
//   - error: Any error during table existence check or creation
func (c *Client) ensureTableExists(ctx context.Context) error {
	log.Printf("Checking if DynamoDB table '%s' exists", c.tableName)

	// Try to describe the table to check if it exists
	_, err := c.dynamoClient.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(c.tableName),
	})

	if err == nil {
		// Table exists
		log.Printf("DynamoDB table '%s' already exists", c.tableName)
		return nil
	}

	// Check if the error is ResourceNotFoundException (table doesn't exist)
	var notFoundErr *types.ResourceNotFoundException
	if !errors.As(err, &notFoundErr) {
		// Some other error occurred (permissions, network, etc.)
		return fmt.Errorf("failed to describe table: %w", err)
	}

	// Table doesn't exist, create it
	log.Printf("DynamoDB table '%s' does not exist, creating it", c.tableName)

	// Create the table with team_id as partition key
	createInput := &dynamodb.CreateTableInput{
		TableName: aws.String(c.tableName),
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("team_id"),
				AttributeType: types.ScalarAttributeTypeS, // String type
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("team_id"),
				KeyType:       types.KeyTypeHash, // Partition key
			},
		},
		BillingMode: types.BillingModePayPerRequest, // Pay-per-request (no capacity planning needed)
		Tags: []types.Tag{
			{
				Key:   aws.String("Application"),
				Value: aws.String("soccer-scraper"),
			},
			{
				Key:   aws.String("Purpose"),
				Value: aws.String("Schedule storage and notification management"),
			},
		},
	}

	_, err = c.dynamoClient.CreateTable(ctx, createInput)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	log.Printf("DynamoDB table '%s' creation initiated", c.tableName)

	// Wait for the table to become active
	waiter := dynamodb.NewTableExistsWaiter(c.dynamoClient)
	err = waiter.Wait(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(c.tableName),
	}, 2*time.Minute) // Wait up to 2 minutes for table to become active

	if err != nil {
		return fmt.Errorf("failed waiting for table to become active: %w", err)
	}

	log.Printf("DynamoDB table '%s' is now active", c.tableName)

	// Enable TTL on the 'ttl' attribute for automatic data cleanup
	err = c.enableTTL(ctx)
	if err != nil {
		// Log warning but don't fail - TTL can be enabled manually if needed
		log.Printf("Warning: failed to enable TTL on table '%s': %v. TTL can be enabled manually.", c.tableName, err)
	}

	log.Printf("DynamoDB table '%s' successfully created and configured", c.tableName)
	return nil
}

// enableTTL enables Time To Live (TTL) on the 'ttl' attribute.
// This allows DynamoDB to automatically delete expired items.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//
// Returns:
//   - error: Any error during TTL enablement
func (c *Client) enableTTL(ctx context.Context) error {
	log.Printf("Enabling TTL on table '%s'", c.tableName)

	_, err := c.dynamoClient.UpdateTimeToLive(ctx, &dynamodb.UpdateTimeToLiveInput{
		TableName: aws.String(c.tableName),
		TimeToLiveSpecification: &types.TimeToLiveSpecification{
			Enabled:       aws.Bool(true),
			AttributeName: aws.String("ttl"),
		},
	})

	if err != nil {
		return fmt.Errorf("failed to enable TTL: %w", err)
	}

	log.Printf("TTL enabled successfully on table '%s'", c.tableName)
	return nil
}

// StoredSchedule represents a team's schedule stored in DynamoDB.
type StoredSchedule struct {
	// TeamID is the primary key - the 6-digit team ID.
	TeamID string `dynamodbav:"team_id"`

	// TeamName is the team's display name.
	TeamName string `dynamodbav:"team_name"`

	// Season is the season identifier.
	Season string `dynamodbav:"season"`

	// Games is the list of games in the schedule.
	Games []StoredGame `dynamodbav:"games"`

	// TopicArn is the SNS topic ARN for this team's notifications.
	TopicArn string `dynamodbav:"topic_arn"`

	// LastChecked is when the schedule was last fetched from the API.
	LastChecked time.Time `dynamodbav:"last_checked"`

	// TTL is the Unix timestamp when this item should be automatically deleted.
	// DynamoDB TTL feature will delete items after this time.
	TTL int64 `dynamodbav:"ttl"`
}

// StoredGame represents a single game in storage.
// Fields are simplified compared to the full Game type.
type StoredGame struct {
	// GameID is the unique identifier for the game.
	GameID string `dynamodbav:"game_id"`

	// DateStr is the ISO 8601 datetime string for the game.
	DateStr string `dynamodbav:"date_str"`

	// Field is the field number/name.
	Field string `dynamodbav:"field"`

	// HomeTeam is the home team name.
	HomeTeam string `dynamodbav:"home_team"`

	// AwayTeam is the away team name.
	AwayTeam string `dynamodbav:"away_team"`
}

// SaveSchedule stores a team's schedule in DynamoDB.
// It sets a TTL for automatic cleanup after the season.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - schedule: The schedule to store
//
// Returns:
//   - error: Any error during storage
func (c *Client) SaveSchedule(ctx context.Context, schedule *StoredSchedule) error {
	// Set TTL to DefaultTTLDuration from now if not already set
	if schedule.TTL == 0 {
		schedule.TTL = time.Now().Add(DefaultTTLDuration).Unix()
	}

	// Set the last checked time
	schedule.LastChecked = time.Now()

	log.Printf("Saving schedule for team %s with %d games (TTL: %d)",
		schedule.TeamID, len(schedule.Games), schedule.TTL)

	// Marshal the schedule to DynamoDB attribute values
	item, err := attributevalue.MarshalMap(schedule)
	if err != nil {
		return fmt.Errorf("failed to marshal schedule: %w", err)
	}

	// Store in DynamoDB
	_, err = c.dynamoClient.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(c.tableName),
		Item:      item,
	})
	if err != nil {
		return fmt.Errorf("failed to save schedule for team %s: %w", schedule.TeamID, err)
	}

	log.Printf("Schedule saved successfully for team %s", schedule.TeamID)
	return nil
}

// GetSchedule retrieves a team's stored schedule from DynamoDB.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - teamID: The team ID to retrieve the schedule for
//
// Returns:
//   - *StoredSchedule: The stored schedule, or nil if not found
//   - error: Any error during retrieval (not including "not found")
func (c *Client) GetSchedule(ctx context.Context, teamID string) (*StoredSchedule, error) {
	log.Printf("Retrieving schedule for team %s", teamID)

	result, err := c.dynamoClient.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(c.tableName),
		Key: map[string]types.AttributeValue{
			"team_id": &types.AttributeValueMemberS{Value: teamID},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get schedule for team %s: %w", teamID, err)
	}

	// Check if item exists
	if result.Item == nil {
		log.Printf("No stored schedule found for team %s", teamID)
		return nil, nil
	}

	// Unmarshal the result
	var schedule StoredSchedule
	err = attributevalue.UnmarshalMap(result.Item, &schedule)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal schedule: %w", err)
	}

	log.Printf("Retrieved schedule for team %s with %d games", teamID, len(schedule.Games))
	return &schedule, nil
}

// DeleteSchedule removes a team's schedule from DynamoDB.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - teamID: The team ID to delete the schedule for
//
// Returns:
//   - error: Any error during deletion
func (c *Client) DeleteSchedule(ctx context.Context, teamID string) error {
	log.Printf("Deleting schedule for team %s", teamID)

	_, err := c.dynamoClient.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(c.tableName),
		Key: map[string]types.AttributeValue{
			"team_id": &types.AttributeValueMemberS{Value: teamID},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to delete schedule for team %s: %w", teamID, err)
	}

	log.Printf("Schedule deleted successfully for team %s", teamID)
	return nil
}

// ConvertGamesToStoredGames converts types.Game slice to StoredGame slice.
// This extracts only the fields needed for storage and comparison.
//
// Parameters:
//   - games: Slice of Game structs from the LPS API
//
// Returns:
//   - []StoredGame: Slice of StoredGame structs for storage
func ConvertGamesToStoredGames(games []sharedtypes.Game) []StoredGame {
	storedGames := make([]StoredGame, len(games))
	for i, g := range games {
		storedGames[i] = StoredGame{
			GameID:   g.GameID,
			DateStr:  g.DateStr,
			Field:    g.Field,
			HomeTeam: g.HomeTeam,
			AwayTeam: g.AwayTeam,
		}
	}
	return storedGames
}

// ListAllSchedules retrieves all stored schedules from DynamoDB with pagination.
// This is useful for the scheduled checker to find all teams to check.
// Uses pagination to handle large datasets efficiently.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//
// Returns:
//   - []StoredSchedule: All stored schedules
//   - error: Any error during retrieval
func (c *Client) ListAllSchedules(ctx context.Context) ([]StoredSchedule, error) {
	log.Printf("Listing all stored schedules")

	var schedules []StoredSchedule
	var lastEvaluatedKey map[string]types.AttributeValue

	// Use pagination with a reasonable page size to prevent memory issues
	// and improve performance for larger datasets
	const pageSize int32 = 25

	for {
		input := &dynamodb.ScanInput{
			TableName: aws.String(c.tableName),
			Limit:     aws.Int32(pageSize),
		}
		if lastEvaluatedKey != nil {
			input.ExclusiveStartKey = lastEvaluatedKey
		}

		result, err := c.dynamoClient.Scan(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("failed to scan schedules: %w", err)
		}

		// Unmarshal results
		for _, item := range result.Items {
			var schedule StoredSchedule
			err = attributevalue.UnmarshalMap(item, &schedule)
			if err != nil {
				log.Printf("Warning: failed to unmarshal schedule item: %v", err)
				continue
			}
			schedules = append(schedules, schedule)
		}

		log.Printf("Scanned page with %d items (total so far: %d)", len(result.Items), len(schedules))

		// Check if there are more items
		if result.LastEvaluatedKey == nil {
			break
		}
		lastEvaluatedKey = result.LastEvaluatedKey
	}

	log.Printf("Found %d stored schedules", len(schedules))
	return schedules, nil
}

// HasTeams checks if there are any teams stored in DynamoDB.
// This is a cost-optimized check that scans for existence without reading item data.
// Used by EventBridge scheduled rules to skip invocations when there are no subscribed teams.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//
// Returns:
//   - bool: True if there is at least one team stored
//   - error: Any error during the check
func (c *Client) HasTeams(ctx context.Context) (bool, error) {
	log.Printf("Checking if there are any stored teams")

	// Use Scan with Limit 1 to minimize read costs
	// Check if Count > 0 to determine if any teams exist
	result, err := c.dynamoClient.Scan(ctx, &dynamodb.ScanInput{
		TableName: aws.String(c.tableName),
		Limit:     aws.Int32(1),
	})
	if err != nil {
		return false, fmt.Errorf("failed to check for teams: %w", err)
	}

	hasTeams := result.Count > 0
	log.Printf("Has teams: %v (count: %d)", hasTeams, result.Count)
	return hasTeams, nil
}
