from bs4 import BeautifulSoup  # Keep for potential future HTML parsing needs
import requests
from ics import Calendar, Event
from datetime import datetime, timedelta, timezone
import re
import json
import pandas

"""
Soccer Schedule Scraper Lambda Function

This Lambda function provides two main functionalities:
1. Fetch soccer game schedules for specified team IDs from the LPS API
2. Generate downloadable ICS calendar files for the retrieved games

It's designed to be called from the portfolio website through AWS Lambda,
with authentication handled via Cognito Identity Pool.
"""

def validate_team_id(team_id: str) -> bool:
    """
    Validate that a team ID is properly formatted.
    
    Args:
        team_id (str): The team ID to validate
        
    Returns:
        bool: True if the team ID is valid
        
    Raises:
        ValueError: If the team ID is invalid with specific error message
    """
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
    """
    Fetch team schedule data from the LPS API.
    
    Args:
        team_id (str): The 6-digit team ID to fetch schedule for
        
    Returns:
        tuple: (list of game dictionaries, season string, team_name string)
        
    Raises:
        ValueError: For validation or data structure errors
        RuntimeError: For network or parsing errors
    """
    # Validate team ID before making request
    try:
        validate_team_id(team_id)
    except ValueError as e:
        raise ValueError(str(e))
    
    # URL of the API endpoint
    url = f"https://lps-api-prod.lps-test.com/teams/{team_id}"

    # Fetch the data from API
    try:
        response = requests.get(url, timeout=10)  # Add timeout for better error handling
        response.raise_for_status()  # Raise exception for bad status codes
    except requests.Timeout:
        raise RuntimeError(f"Request timed out while fetching schedule for team {team_id}. Please try again.")
    except requests.ConnectionError:
        raise RuntimeError(f"Connection error while fetching schedule for team {team_id}. Please check your internet connection.")
    except requests.RequestException as e:
        raise RuntimeError(f"Could not fetch any schedule, make sure team ID is accurate.")
    
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
    
    # Extract the season number and team name directly from API response
    team_data = data["team"]
    SEASON = str(team_data.get("Season", "Unknown"))
    TEAM_NAME = team_data.get("team_name", "Unknown Team")
    
    # Debug log
    print(f"Team Name: {TEAM_NAME}, Season: {SEASON}")
    
    # Get games data
    if "games" not in data or not isinstance(data["games"], list):
        raise ValueError(f"No games data found for team {team_id}")
    
    # Process games
    all_games = []
    # Get current date with timezone info to match the game dates
    mt_offset = -7  # Mountain Time offset
    tz = timezone(timedelta(hours=mt_offset))
    current_date = (pandas.to_datetime(datetime.now(tz))).round('min')
    
    print(f"Current date: {current_date}")  # Debug log
    
    for game in data["games"]:
        try:
            # Extract game details
            game_id = game.get("game_id", "")
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
                game_date = datetime.fromisoformat(game_datetime.replace("Z", "-07:00"))
                
                # Format the date as it was in the original scraper
                formatted_date = game_date.strftime("%a %m/%d %I:%M %p")
                
                # Convert to string with timezone info for passing to ICS
                game_str = game_date.isoformat()

                # Only show future games (may add an option to include past games)
                if game_date >= current_date:
                    all_games.append({
                        'game_id': game_id,
                        'date': formatted_date,
                        'date_str': game_str,
                        'field': field,
                        'home_team': home_team,
                        'away_team': away_team
                    })
            except ValueError as e:
                print(f"Warning: Error parsing date for game: {game_datetime} - {e}")
                continue
                
        except Exception as e:
            print(f"Warning: Error parsing game data for team {team_id}: {e}")
            continue
    
    if not all_games:
        raise ValueError(f"No upcoming games found for the provided team.")
    
    print(f"Found {len(all_games)} games for team {team_id}")
    return all_games, SEASON, TEAM_NAME

def create_calendar_events(selected_games):
    """
    Create an ICS calendar file from a list of games with proper Mountain Time zone.
    
    Args:
        selected_games (list): List of game dictionaries
        
    Returns:
        str: ICS calendar content as a string with proper timezone handling
    """
    cal = Calendar()
    mt_offset = -7
    tz = timezone(timedelta(hours=mt_offset))
    
    # Add calendar metadata
    cal.creator = 'Soccer Schedule API'
    
    for game in selected_games:
        event = Event()
        
        # Convert the game date string to a datetime object with explicit Mountain Time zone
        game_datetime = datetime.fromisoformat(game['date_str'])
        
        # Ensure the datetime has Mountain Time timezone info
        if game_datetime.tzinfo is None:
            game_datetime = game_datetime.replace(tzinfo=tz)
        
        # Add 7 hours to match UTC time as that is what will be serialized when making the ics file
        utc_gametime = game_datetime - timedelta(hours=7)  # Add 7 hours to match UTC time
        
        # List of special teams
        special_teams = ['MIXED BAG FC', 'LOOKING TO SCORE', 'NO BUENO O30', 'EYE CANDY']

        event.name = f"{game['home_team']} vs {game['away_team']}"
        event.begin = utc_gametime
        event.duration = {'hours': .75}  # 45 minutes duration
        event.location = f"Let's Play Soccer, Boise, 11448 W President Dr #8967, Boise, ID 83713, USA"
        event.description = f"Field {game['field']}\nSoccer game at Let's Play Soccer\n{game['home_team']} vs {game['away_team']}\nGLHF!"

        if game['home_team'] in special_teams or game['away_team'] in special_teams:
            event.name = f"Special Event: {game['home_team']} vs {game['away_team']}"
            event.description = f"Field {game['field']}\nSoccer game at Let's Play Soccer\n{game['home_team']} vs {game['away_team']}\nAhhh shit, here we go again..."
        
        cal.events.add(event)
    
    # Serialize the calendar to get the basic structure
    ics_content = cal.serialize()
    
    # Fix timezone issues by replacing UTC timestamps with Mountain Time
    # 1. Add the timezone definition
    timezone_block = """
BEGIN:VTIMEZONE
TZID:America/Denver
BEGIN:STANDARD
DTSTART:20241103T020000
TZOFFSETFROM:-0600
TZOFFSETTO:-0700
RRULE:FREQ=YEARLY;BYMONTH=11;BYDAY=1SU
END:STANDARD
BEGIN:DAYLIGHT
DTSTART:20250309T020000
TZOFFSETFROM:-0700
TZOFFSETTO:-0600
RRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=2SU
END:DAYLIGHT
END:VTIMEZONE
"""
    
    # Insert the timezone block after BEGIN:VCALENDAR
    ics_content = ics_content.replace("BEGIN:VCALENDAR", f"BEGIN:VCALENDAR\n{timezone_block}")
    
    # 2. Replace UTC timestamps with Mountain Time
    # This pattern matches DTSTART with UTC time (Z suffix)
    pattern_dt = r'DTSTART:(\d{8}T\d{6})Z'
    
    # Replace with TZID parameter format
    replacement_dt = r'DTSTART;TZID=America/Denver:\1'
    ics_content = re.sub(pattern_dt, replacement_dt, ics_content)
    
    # 3. Add VALARM components for reminders
    pattern_valarm = r'(END:VEVENT)'
    valarm_block = """
BEGIN:VALARM
ACTION:DISPLAY
DESCRIPTION:Reminder: Soccer game starting soon
TRIGGER:-PT40M
END:VALARM
"""
    ics_content = re.sub(pattern_valarm, f"{valarm_block}\\1", ics_content)
    
    return ics_content

def lambda_handler(event, context):
    """
    AWS Lambda function handler.
    
    Supports two actions:
    - 'fetch': Get games for specified team IDs
    - 'download': Generate downloadable ICS calendar file
    
    Args:
        event (dict): Lambda event data
        context: Lambda context
        
    Returns:
        dict: Lambda response with appropriate status code and body
    """
    # Add version identifier for logging
    print(f"Soccer Schedule API Version: 2025-03-03-v6")
    
    query_params = event.get('queryStringParameters', {}) or {}  # Handle None case
    action = query_params.get('action', 'fetch')
    
    if action == 'fetch':
        # FETCH ACTION: Get schedules for provided team IDs
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
        
        # Process valid team IDs
        all_games = []
        failed_teams = []
        
        try:
            for team_id in valid_team_ids:
                try:
                    games, season, team_name = get_team_schedule_from_api(team_id)
                    
                    # Add team_id and season to each game for reference
                    for game in games:
                        game['team_id'] = team_id
                        game['season'] = season
                        game['team_name'] = team_name  # Add the team name from API to each game
                        game['id'] = f"{season}_{game['date']}_{game['home_team']}_{game['away_team']}_{game['field']}"
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
        # DOWNLOAD ACTION: Generate ICS calendar file
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
                    'Content-Disposition': 'attachment; filename="soccer_schedule_{season}.ics"'
                },
                'body': calendar_text
            }
            
        except Exception as e:
            print(f"Error generating calendar: {str(e)}")  # Add logging
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
    
    # Default error for invalid action
    return {
        'statusCode': 400,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps({'error': 'Invalid action'})
    }

if __name__ == "__main__":
    """
    CLI mode for local testing of the soccer schedule scraper.
    This allows testing the Lambda functionality from the command line.
    """
    team_ids = input("Enter team IDs (space separated): ").split()
    processed = []
    failed = []
    
    for team_id in team_ids:
        print(f"\nProcessing team ID: {team_id}")
        try:
            # Get schedule data including team name directly from API
            games, season, team_name = get_team_schedule_from_api(team_id)
            print(f"Team: {team_name}")
            print(f"Season: {season}")
            print(f"Found {len(games)} games:")
            
            # Print game details
            for game in games:
                print(f"\nDate/Time: {game['date']}")
                print(f"Field: {game['field']}")
                print(f"Home Team: {game['home_team']}")
                print(f"Away Team: {game['away_team']}")
                print("-" * 40)
            
            # Create calendar using team name from API response
            calendar_text = create_calendar_events(games)
            calendar_file = f"{season}_{team_name}_{team_id}.ics"
            
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

