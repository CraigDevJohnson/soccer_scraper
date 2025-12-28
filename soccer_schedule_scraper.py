from bs4 import BeautifulSoup
import requests
from ics import Calendar, Event
from datetime import datetime, timedelta, timezone
import re
import json
import os
import boto3
from decimal import Decimal

# AWS clients initialization (lazy-loaded)
_sns_client = None
_dynamodb = None

# Environment variables
SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN', '')
DYNAMODB_TABLE_NAME = os.environ.get('DYNAMODB_TABLE', 'soccer_schedules')

def get_sns_client():
    """Get SNS client (lazy initialization)."""
    global _sns_client
    if _sns_client is None:
        _sns_client = boto3.client('sns')
    return _sns_client

def get_dynamodb_resource():
    """Get DynamoDB resource (lazy initialization)."""
    global _dynamodb
    if _dynamodb is None:
        _dynamodb = boto3.resource('dynamodb')
    return _dynamodb

def get_dynamodb_table():
    """Get DynamoDB table reference."""
    dynamodb = get_dynamodb_resource()
    return dynamodb.Table(DYNAMODB_TABLE_NAME)

def subscribe_email_to_topic(email, team_ids):
    """
    Subscribe an email address to SNS topic for schedule change notifications.
    
    Args:
        email: Email address to subscribe
        team_ids: List of team IDs to monitor
        
    Returns:
        dict: Subscription details including subscription ARN
    """
    try:
        # Validate email format
        if not re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email):
            raise ValueError(f"Invalid email format: {email}")
        
        # Check if already subscribed
        sns = get_sns_client()
        response = sns.list_subscriptions_by_topic(TopicArn=SNS_TOPIC_ARN)
        for sub in response.get('Subscriptions', []):
            if sub.get('Endpoint') == email and sub.get('Protocol') == 'email':
                # Update team_ids in DynamoDB for existing subscription
                table = get_dynamodb_table()
                table.put_item(
                    Item={
                        'team_id': f'subscription#{email}',
                        'game_id': 'metadata',
                        'monitored_teams': team_ids,
                        'email': email,
                        'subscription_arn': sub.get('SubscriptionArn'),
                        'updated_at': datetime.now(timezone.utc).isoformat()
                    }
                )
                return {
                    'status': 'already_subscribed',
                    'email': email,
                    'subscription_arn': sub.get('SubscriptionArn'),
                    'message': 'Email already subscribed. Updated monitored teams.'
                }
        
        # Subscribe to SNS topic
        response = sns.subscribe(
            TopicArn=SNS_TOPIC_ARN,
            Protocol='email',
            Endpoint=email
        )
        
        subscription_arn = response.get('SubscriptionArn', 'pending confirmation')
        
        # Store subscription info in DynamoDB
        table = get_dynamodb_table()
        table.put_item(
            Item={
                'team_id': f'subscription#{email}',
                'game_id': 'metadata',
                'monitored_teams': team_ids,
                'email': email,
                'subscription_arn': subscription_arn,
                'created_at': datetime.now(timezone.utc).isoformat(),
                'updated_at': datetime.now(timezone.utc).isoformat(),
                'ttl': int((datetime.now(timezone.utc) + timedelta(days=90)).timestamp())
            }
        )
        
        return {
            'status': 'subscribed',
            'email': email,
            'subscription_arn': subscription_arn,
            'message': 'Subscription created. Please check your email to confirm.'
        }
        
    except Exception as e:
        print(f"Error subscribing email {email}: {str(e)}")
        raise

def unsubscribe_email_from_topic(email):
    """
    Unsubscribe an email address from SNS topic.
    
    Args:
        email: Email address to unsubscribe
        
    Returns:
        dict: Unsubscription status
    """
    try:
        # Find subscription ARN for this email
        sns = get_sns_client()
        response = sns.list_subscriptions_by_topic(TopicArn=SNS_TOPIC_ARN)
        
        subscription_arn = None
        for sub in response.get('Subscriptions', []):
            if sub.get('Endpoint') == email and sub.get('Protocol') == 'email':
                subscription_arn = sub.get('SubscriptionArn')
                break
        
        if not subscription_arn or subscription_arn == 'PendingConfirmation':
            # Remove from DynamoDB even if not confirmed
            table = get_dynamodb_table()
            table.delete_item(
                Key={
                    'team_id': f'subscription#{email}',
                    'game_id': 'metadata'
                }
            )
            return {
                'status': 'not_found',
                'email': email,
                'message': 'No active subscription found for this email.'
            }
        
        # Unsubscribe from SNS
        sns.unsubscribe(SubscriptionArn=subscription_arn)
        
        # Remove from DynamoDB
        table = get_dynamodb_table()
        table.delete_item(
            Key={
                'team_id': f'subscription#{email}',
                'game_id': 'metadata'
            }
        )
        
        return {
            'status': 'unsubscribed',
            'email': email,
            'message': 'Successfully unsubscribed from notifications.'
        }
        
    except Exception as e:
        print(f"Error unsubscribing email {email}: {str(e)}")
        raise

def store_schedule_in_dynamodb(team_id, games):
    """
    Store game schedule in DynamoDB for future comparison.
    
    Args:
        team_id: Team ID
        games: List of game dictionaries
    """
    try:
        table = get_dynamodb_table()
        
        # Store each game
        for game in games:
            # TTL set to 90 days from now (use UTC for consistent timestamps)
            ttl = int((datetime.now(timezone.utc) + timedelta(days=90)).timestamp())
            
            table.put_item(
                Item={
                    'team_id': team_id,
                    'game_id': game['id'],
                    'date': game['date'],
                    'field': game['field'],
                    'home_team': game['home_team'],
                    'away_team': game['away_team'],
                    'season': game.get('season', 'Unknown'),
                    'updated_at': datetime.now(timezone.utc).isoformat(),
                    'ttl': ttl
                }
            )
        
        print(f"Stored {len(games)} games for team {team_id} in DynamoDB")
        
    except Exception as e:
        print(f"Error storing schedule in DynamoDB: {str(e)}")
        raise

def get_stored_schedule_from_dynamodb(team_id):
    """
    Retrieve stored schedule from DynamoDB.
    
    Args:
        team_id: Team ID
        
    Returns:
        dict: Dictionary of game_id -> game data
    """
    try:
        table = get_dynamodb_table()
        
        response = table.query(
            KeyConditionExpression='team_id = :tid',
            ExpressionAttributeValues={
                ':tid': team_id
            }
        )
        
        # Convert to dictionary keyed by game_id
        stored_games = {}
        for item in response.get('Items', []):
            game_id = item.get('game_id')
            # Skip subscription metadata
            if game_id == 'metadata':
                continue
            stored_games[game_id] = item
        
        return stored_games
        
    except Exception as e:
        print(f"Error retrieving schedule from DynamoDB: {str(e)}")
        return {}

def compare_schedules(old_schedule, new_schedule):
    """
    Compare old and new schedules to detect changes.
    
    Args:
        old_schedule: Dictionary of game_id -> game data (from DynamoDB)
        new_schedule: List of new game dictionaries
        
    Returns:
        dict: Changes detected (added, removed, modified games)
    """
    changes = {
        'added': [],
        'removed': [],
        'modified': []
    }
    
    # Convert new schedule to dictionary for easier comparison
    new_games_dict = {game['id']: game for game in new_schedule}
    
    # Check for added and modified games
    for game_id, new_game in new_games_dict.items():
        if game_id not in old_schedule:
            changes['added'].append(new_game)
        else:
            old_game = old_schedule[game_id]
            # Compare key fields
            if (old_game.get('date') != new_game.get('date') or
                old_game.get('field') != new_game.get('field') or
                old_game.get('home_team') != new_game.get('home_team') or
                old_game.get('away_team') != new_game.get('away_team')):
                changes['modified'].append({
                    'old': old_game,
                    'new': new_game
                })
    
    # Check for removed games
    for game_id in old_schedule:
        if game_id not in new_games_dict:
            changes['removed'].append(old_schedule[game_id])
    
    return changes

def send_schedule_change_notification(team_id, changes):
    """
    Send SNS notification about schedule changes.
    
    Args:
        team_id: Team ID
        changes: Dictionary of changes (from compare_schedules)
    """
    try:
        # Build notification message
        message_lines = [f"Schedule changes detected for team {team_id}:\n"]
        
        if changes['added']:
            message_lines.append("\n🆕 NEW GAMES:")
            for game in changes['added']:
                message_lines.append(
                    f"  • {game['date']} - Field {game['field']}: "
                    f"{game['home_team']} vs {game['away_team']}"
                )
        
        if changes['modified']:
            message_lines.append("\n📝 MODIFIED GAMES:")
            for change in changes['modified']:
                old = change['old']
                new = change['new']
                message_lines.append(f"  Game: {new['home_team']} vs {new['away_team']}")
                if old.get('date') != new.get('date'):
                    message_lines.append(f"    Date: {old.get('date')} → {new.get('date')}")
                if old.get('field') != new.get('field'):
                    message_lines.append(f"    Field: {old.get('field')} → {new.get('field')}")
        
        if changes['removed']:
            message_lines.append("\n❌ CANCELLED GAMES:")
            for game in changes['removed']:
                message_lines.append(
                    f"  • {game['date']} - Field {game['field']}: "
                    f"{game['home_team']} vs {game['away_team']}"
                )
        
        message = "\n".join(message_lines)
        
        # Send notification
        sns = get_sns_client()
        response = sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=f"Soccer Schedule Update - Team {team_id}",
            Message=message
        )
        
        print(f"Sent schedule change notification for team {team_id}")
        return response
        
    except Exception as e:
        print(f"Error sending notification: {str(e)}")
        raise

def check_schedules_for_changes():
    """
    Check all monitored team schedules for changes.
    Called periodically by EventBridge.
    
    Returns:
        dict: Summary of checks performed
    """
    try:
        # Get all subscriptions from DynamoDB
        table = get_dynamodb_table()
        response = table.scan(
            FilterExpression='begins_with(team_id, :prefix)',
            ExpressionAttributeValues={
                ':prefix': 'subscription#'
            }
        )
        
        subscriptions = response.get('Items', [])
        print(f"Found {len(subscriptions)} active subscriptions")
        
        results = {
            'checked_teams': [],
            'changes_detected': [],
            'errors': []
        }
        
        # Get unique team IDs from all subscriptions
        all_team_ids = set()
        for sub in subscriptions:
            monitored_teams = sub.get('monitored_teams', [])
            all_team_ids.update(monitored_teams)
        
        print(f"Checking {len(all_team_ids)} unique teams")
        
        # Check each team for changes
        for team_id in all_team_ids:
            try:
                # Get current schedule
                current_games, season = get_team_schedule_from_api(team_id)
                
                # Add IDs to games using ISO datetime for stability
                for game in current_games:
                    game['team_id'] = team_id
                    game['season'] = season
                    # Use ISO datetime for stable ID generation instead of formatted date
                    game['id'] = f"{season}_{game.get('game_datetime_iso', game['date'])}_{game['home_team']}_{game['away_team']}_{game['field']}"
                
                # Get stored schedule
                stored_schedule = get_stored_schedule_from_dynamodb(team_id)
                
                # Compare schedules
                if stored_schedule:
                    changes = compare_schedules(stored_schedule, current_games)
                    
                    # Send notification if changes detected
                    if changes['added'] or changes['modified'] or changes['removed']:
                        send_schedule_change_notification(team_id, changes)
                        results['changes_detected'].append({
                            'team_id': team_id,
                            'changes': changes
                        })
                
                # Store current schedule
                store_schedule_in_dynamodb(team_id, current_games)
                results['checked_teams'].append(team_id)
                
            except Exception as e:
                print(f"Error checking team {team_id}: {str(e)}")
                results['errors'].append({
                    'team_id': team_id,
                    'error': str(e)
                })
        
        return results
        
    except Exception as e:
        print(f"Error in check_schedules_for_changes: {str(e)}")
        raise

def validate_team_id(team_id: str) -> bool:
    """Validate that a team ID is properly formatted."""
    if not isinstance(team_id, str):
        raise ValueError("Team ID must be a string")
    if not team_id.strip():
        raise ValueError("Team ID cannot be empty")
    if not re.match(r'^\d{6}$', team_id):
        raise ValueError(f"Team ID '{team_id}' must be exactly 6 digits")
    # Ensure it's a positive integer when parsed
    try:
        num_id = int(team_id)
        if num_id <= 0:
            raise ValueError(f"Team ID '{team_id}' must be a positive number")
        return True
    except ValueError as e:
        if "must be" in str(e):
            raise e
        raise ValueError(f"Team ID '{team_id}' must be a valid number")

def get_team_schedule_from_api(team_id):
    # Validate team ID before making request
    try:
        validate_team_id(team_id)
    except ValueError as e:
        raise ValueError(str(e))
    
    # URL of the API endpoint
    url = f"https://lps-api-prod.lps-test.com/teams/{team_id}"
    
    # Fetch the data from API
    try:
        response = requests.get(url, timeout=10)  # Add timeout
        response.raise_for_status()  # Raise exception for bad status codes
    except requests.Timeout:
        raise RuntimeError(f"Request timed out while fetching schedule for team {team_id}. Please try again.")
    except requests.ConnectionError:
        raise RuntimeError(f"Connection error while fetching schedule for team {team_id}. Please check your internet connection.")
    except requests.RequestException as e:
        raise RuntimeError(f"Failed to fetch schedule for team {team_id}: {str(e)}")
    
    # Parse JSON response
    try:
        data = response.json()
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Failed to parse API response for team {team_id}: {str(e)}")
    
    # Check if the response contains the expected data
    if not data or not isinstance(data, dict):
        raise ValueError(f"Invalid API response for team {team_id}")
    
    # Check if team data exists
    if "team" not in data:
        raise ValueError(f"Team ID {team_id} not found. Please verify the team code is correct.")
    
    # Extract the season number
    team_data = data["team"]
    SEASON = str(team_data.get("Season", "Unknown"))
    
    # Get games data
    if "games" not in data or not isinstance(data["games"], list):
        raise ValueError(f"No games data found for team {team_id}")
    
    # Process games
    all_games = []
    # Get current date with timezone info to match the game dates
    mt_offset = -7
    tz = timezone(timedelta(hours=mt_offset))
    current_date = datetime.now(tz)
    
    print(f"Current date: {current_date}")  # Debug log
    
    for game in data["games"]:
        try:
            # Extract game details
            game_datetime = game.get("SchedGameDateTime")
            field = game.get("field_name", "").replace("Field ", "") if game.get("field_name") else str(game.get("Field", ""))
            
            # Get home and away team info
            home_team = game.get("home_team", {}).get("team_name", "")
            away_team = game.get("visitor_team", {}).get("team_name", "")
            
            if not all([game_datetime, field, home_team, away_team]):
                print(f"Warning: Missing game data for game in team {team_id}")
                continue
            
            # Parse the game datetime from ISO format
            try:
                game_date = datetime.fromisoformat(game_datetime.replace("Z", "+00:00"))
                
                # Convert to local time (assuming MT timezone for consistency with original code)
                game_date = game_date.astimezone(tz)
                
                # Format the date as it was in the original scraper
                formatted_date = game_date.strftime("%a %m/%d %I:%M %p")
                
                # Only show future games (may add an option to include past games)
                if game_date >= current_date:
                    all_games.append({
                        'date': formatted_date,
                        'field': field,
                        'home_team': home_team,
                        'away_team': away_team,
                        'game_datetime_iso': game_datetime  # Store ISO datetime for stable ID generation
                    })
            except ValueError as e:
                print(f"Warning: Error parsing date for game: {game_datetime} - {e}")
                continue
                
        except Exception as e:
            print(f"Warning: Error parsing game data for team {team_id}: {e}")
            continue
    
    if not all_games:
        raise ValueError(f"No games found for team {team_id}.")
    
    print(f"Found {len(all_games)} games for team {team_id}")
    return all_games, SEASON

def create_calendar_events(selected_games):
    cal = Calendar()
    mt_offset = -7
    tz = timezone(timedelta(hours=mt_offset))
    
    # Add calendar metadata
    cal.creator = 'Soccer Schedule API'
    
    # Determine the season year based on the first game
    current_date = datetime.now()
    current_year = current_date.year
    
    # Sort games to find first game date
    sorted_games = sorted(selected_games, key=lambda x: datetime.strptime(x['date'], "%a %m/%d %I:%M %p"))
    if sorted_games:
        first_game = datetime.strptime(sorted_games[0]['date'], "%a %m/%d %I:%M %p")
        first_game = first_game.replace(year=current_year)
        
        # If first game is more than a week in the past, use next year
        one_week_ago = current_date - timedelta(days=7)
        if first_game < one_week_ago:
            current_year += 1
    
    for game in selected_games:
        event = Event()
        date_str = game['date']
        dt = datetime.strptime(f"{date_str} {current_year}", "%a %m/%d %I:%M %p %Y")
        dt = dt.replace(tzinfo=tz)
        
        event.name = f"{game['home_team']} vs {game['away_team']}"
        event.begin = dt
        event.duration = {'hours': .75}
        event.location = f"Let's Play Soccer, Boise, 11448 W President Dr #8967, Boise, ID 83713, USA"
        event.description = f"Soccer game at Let's Play Soccer\nField {game['field']}\n{game['home_team']} vs {game['away_team']}"
        
        # We'll add alarms later by directly editing the ICS output
        cal.events.add(event)
    
    # Serialize the calendar to get the basic structure
    ics_content = cal.serialize()
    
    # Inject proper VALARM components for each VEVENT
    # Find all VEVENT blocks and add a VALARM with 40-minute reminder to each
    pattern = r'(END:VEVENT)'
    valarm_block = """
BEGIN:VALARM
ACTION:DISPLAY
DESCRIPTION:Reminder: Soccer game starting soon
TRIGGER:-PT40M
END:VALARM
"""
    # Insert the VALARM block before each END:VEVENT
    ics_content = re.sub(pattern, f"{valarm_block}\\1", ics_content)
    
    return ics_content

def lambda_handler(event, context):
    # Add version identifier
    print(f"Soccer Schedule API Version: 2025-03-02-v3")
    
    query_params = event.get('queryStringParameters', {})
    action = query_params.get('action', 'fetch')
    
    if action == 'fetch':
        team_ids_param = query_params.get('team_ids')
        if not team_ids_param:
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'error': 'Team IDs are required. Please provide at least one valid 6-digit team ID.',
                    'errorType': 'ValidationError'
                })
            }
        
        # Split and clean team IDs
        team_ids = [tid.strip() for tid in team_ids_param.split(',') if tid.strip()]
        
        # Validate and deduplicate team IDs with better error reporting
        valid_team_ids = []
        invalid_team_ids = []
        seen_ids = set()
        validation_errors = []
        
        for team_id in team_ids:
            if team_id in seen_ids:
                invalid_team_ids.append({
                    'id': team_id,
                    'reason': 'Duplicate team ID'
                })
                continue
                
            try:
                validate_team_id(team_id)
                valid_team_ids.append(team_id)
                seen_ids.add(team_id)
            except ValueError as e:
                invalid_team_ids.append({
                    'id': team_id,
                    'reason': str(e)
                })
                validation_errors.append(str(e))
        
        if not valid_team_ids:
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'error': 'No valid team IDs provided',
                    'errorType': 'ValidationError',
                    'invalid_ids': invalid_team_ids,
                    'validation_errors': validation_errors
                })
            }
        
        all_games = []
        failed_teams = []
        
        try:
            for team_id in valid_team_ids:
                try:
                    games, season = get_team_schedule_from_api(team_id)
                    
                    # Add team_id and season to each game for reference
                    for game in games:
                        game['team_id'] = team_id
                        game['season'] = season
                        # Use ISO datetime for stable ID generation instead of formatted date
                        game['id'] = f"{season}_{game.get('game_datetime_iso', game['date'])}_{game['home_team']}_{game['away_team']}_{game['field']}"
                        all_games.append(game)
                except Exception as e:
                    failed_teams.append({
                        'team_id': team_id,
                        'error': str(e),
                        'errorType': e.__class__.__name__
                    })
            
            # Return results even if some teams failed or have no future games
            response_body = {
                'games': all_games,
                'processed_team_ids': valid_team_ids
            }
            
            if failed_teams:
                response_body['failed_teams'] = failed_teams
            if invalid_team_ids:
                response_body['invalid_team_ids'] = invalid_team_ids
                
            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps(response_body)
            }
                
        except Exception as e:
            return {
                'statusCode': 500,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'error': f'An unexpected error occurred: {str(e)}',
                    'errorType': e.__class__.__name__,
                    'processed_team_ids': valid_team_ids,
                    'failed_teams': failed_teams,
                    'invalid_team_ids': invalid_team_ids
                })
            }
    
    elif action == 'download':
        try:
            # For POST requests, the games will be in the body
            if event.get('body'):
                try:
                    body = json.loads(event.get('body', '{}'))
                    games = body.get('games', [])
                except json.JSONDecodeError:
                    return {
                        'statusCode': 400,
                        'headers': {
                            'Content-Type': 'application/json',
                            'Access-Control-Allow-Origin': '*'
                        },
                        'body': json.dumps({'error': 'Invalid JSON in request body'})
                    }
            else:
                games = query_params.get('games', [])

            if not games:
                return {
                    'statusCode': 400,
                    'headers': {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*'
                    },
                    'body': json.dumps({'error': 'No games provided for calendar'})
                }
                
            calendar_text = create_calendar_events(games)
            
            # Return the raw calendar data with correct headers
            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'text/calendar',
                    'Access-Control-Allow-Origin': '*',
                    'Content-Disposition': 'attachment; filename="soccer_schedule.ics"'
                },
                'body': calendar_text
            }
            
        except Exception as e:
            print(f"Error generating calendar: {str(e)}")
            return {
                'statusCode': 500,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'error': f'Failed to generate calendar: {str(e)}',
                    'errorType': e.__class__.__name__
                })
            }
    
    elif action == 'subscribe':
        try:
            email = query_params.get('email')
            team_ids_param = query_params.get('team_ids')
            
            if not email:
                return {
                    'statusCode': 400,
                    'headers': {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*'
                    },
                    'body': json.dumps({
                        'error': 'Email address is required',
                        'errorType': 'ValidationError'
                    })
                }
            
            if not team_ids_param:
                return {
                    'statusCode': 400,
                    'headers': {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*'
                    },
                    'body': json.dumps({
                        'error': 'Team IDs are required',
                        'errorType': 'ValidationError'
                    })
                }
            
            # Parse team IDs
            team_ids = [tid.strip() for tid in team_ids_param.split(',') if tid.strip()]
            
            # Validate team IDs
            for team_id in team_ids:
                try:
                    validate_team_id(team_id)
                except ValueError as e:
                    return {
                        'statusCode': 400,
                        'headers': {
                            'Content-Type': 'application/json',
                            'Access-Control-Allow-Origin': '*'
                        },
                        'body': json.dumps({
                            'error': str(e),
                            'errorType': 'ValidationError'
                        })
                    }
            
            # Subscribe email
            result = subscribe_email_to_topic(email, team_ids)
            
            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps(result)
            }
            
        except Exception as e:
            print(f"Error subscribing email: {str(e)}")
            return {
                'statusCode': 500,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'error': f'Failed to subscribe: {str(e)}',
                    'errorType': e.__class__.__name__
                })
            }
    
    elif action == 'unsubscribe':
        try:
            email = query_params.get('email')
            
            if not email:
                return {
                    'statusCode': 400,
                    'headers': {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*'
                    },
                    'body': json.dumps({
                        'error': 'Email address is required',
                        'errorType': 'ValidationError'
                    })
                }
            
            # Unsubscribe email
            result = unsubscribe_email_from_topic(email)
            
            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps(result)
            }
            
        except Exception as e:
            print(f"Error unsubscribing email: {str(e)}")
            return {
                'statusCode': 500,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'error': f'Failed to unsubscribe: {str(e)}',
                    'errorType': e.__class__.__name__
                })
            }
    
    elif action == 'check_schedules':
        # This action is called by EventBridge to check for schedule changes
        try:
            results = check_schedules_for_changes()
            
            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'message': 'Schedule check completed',
                    'results': results
                })
            }
            
        except Exception as e:
            print(f"Error checking schedules: {str(e)}")
            return {
                'statusCode': 500,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'error': f'Failed to check schedules: {str(e)}',
                    'errorType': e.__class__.__name__
                })
            }

    return {
        'statusCode': 400,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps({'error': 'Invalid action'})
    }

if __name__ == "__main__":
    team_ids = input("Enter team IDs (space separated): ").split()
    processed = []
    failed = []
    
    for team_id in team_ids:
        print(f"\nProcessing team ID: {team_id}")
        try:
            games, season = get_team_schedule_from_api(team_id)
            print(f"Season: {season}")
            print(f"Found {len(games)} games:")
            
            # Count team appearances
            team_counts = {}
            for game in games:
                team_counts[game['home_team']] = team_counts.get(game['home_team'], 0) + 1
                team_counts[game['away_team']] = team_counts.get(game['away_team'], 0) + 1
                print(f"\nDate/Time: {game['date']}")
                print(f"Field: {game['field']}")
                print(f"Home Team: {game['home_team']}")
                print(f"Away Team: {game['away_team']}")
                print("-" * 40)
            
            my_team = max(team_counts.items(), key=lambda x: x[1])[0]
            calendar_text = create_calendar_events(games)
            calendar_file = f"{season}_{my_team}_{team_id}.ics"
            
            with open(calendar_file, 'w', newline='\r\n') as f:
                f.write(calendar_text)
            print(f"\nCalendar file '{calendar_file}' created successfully!")
            processed.append(team_id)
            
        except requests.RequestException as e:
            print(f"Error fetching data for team {team_id}: {e}")
            failed.append(team_id)
        except Exception as e:
            print(f"Error processing team {team_id}: {e}")
            failed.append(team_id)
    
    # Show summary
    print("\nProcessing complete!")
    print(f"Successfully processed {len(processed)} teams: {', '.join(processed)}")
    if failed:
        print(f"Failed to process {len(failed)} teams: {', '.join(failed)}")

