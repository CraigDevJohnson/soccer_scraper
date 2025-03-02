from bs4 import BeautifulSoup
import requests
from ics import Calendar, Event
from datetime import datetime, timedelta, timezone
import re
import json

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

def scrape_soccer_schedule(team_id):
    # Validate team ID before making request
    try:
        validate_team_id(team_id)
    except ValueError as e:
        raise ValueError(str(e))
    
    # URL of the schedule page
    url = f"https://www.letsplaysoccer.com/4/teamSchedule/{team_id}"
    
    # Fetch the webpage
    try:
        response = requests.get(url, timeout=10)  # Add timeout
        response.raise_for_status()  # Raise exception for bad status codes
    except requests.Timeout:
        raise RuntimeError(f"Request timed out while fetching schedule for team {team_id}. Please try again.")
    except requests.ConnectionError:
        raise RuntimeError(f"Connection error while fetching schedule for team {team_id}. Please check your internet connection.")
    except requests.RequestException as e:
        raise RuntimeError(f"Failed to fetch schedule for team {team_id}: {str(e)}")
    
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Check if page contains valid team data
    if "Team not found" in response.text:
        raise ValueError(f"Team ID {team_id} not found. Please verify the team code is correct.")
    if "Invalid team" in response.text:
        raise ValueError(f"Team ID {team_id} is invalid. Please verify the team code is correct.")
    
    # Find season number with better error handling
    try:
        team_header = soup.find('h4', class_='text-md-40-24')
        SEASON = "Unknown"
        if team_header:
            next_elem = team_header.find_next_sibling()
            if next_elem:
                text_content = next_elem.get_text(strip=True)
                if 'Season:' in text_content:
                    match = re.search(r'Season:(\d+)', text_content)
                    if match:
                        SEASON = match.group(1)
    except Exception as e:
        print(f"Warning: Error extracting season for team {team_id}: {e}")
        SEASON = "Unknown"

    # Find all table rows with better error handling
    games = []
    rows = soup.find_all('tr')
    
    for row in rows:
        try:
            cells = row.find_all('td')
            if len(cells) == 5:  # Verify it's a game row
                date_time = cells[0].text.strip()
                field = cells[1].text.strip().split(' ')[1]  # Extract just the number
                home_team = cells[2].find('span').text.strip()
                away_team = cells[3].find('span').text.strip()
                
                if not all([date_time, field, home_team, away_team]):
                    print(f"Warning: Skipping game row with missing data for team {team_id}")
                    continue
                
                game = {
                    'date': date_time,
                    'field': field,
                    'home_team': home_team,
                    'away_team': away_team
                }
                games.append(game)
        except Exception as e:
            print(f"Warning: Error parsing game row for team {team_id}: {e}")
            continue
    
    if not games:
        raise ValueError(f"No games found for team {team_id}. The team may not have any scheduled games.")
    
    return games, SEASON

def create_calendar_events(selected_games):
    cal = Calendar()
    mt_offset = -7
    tz = timezone(timedelta(hours=mt_offset))
    
    # Add calendar metadata
    cal.creator = 'Soccer Schedule Scraper'
    
    for game in selected_games:
        event = Event()
        date_str = game['date']
        current_year = datetime.now().year
        dt = datetime.strptime(f"{date_str} {current_year}", "%a %m/%d %I:%M %p %Y")
        dt = dt.replace(tzinfo=tz)
        
        event.name = f"{game['home_team']} vs {game['away_team']}"
        event.begin = dt
        event.duration = {'hours': .75}
        event.location = f"Let's Play Soccer, Boise, 11448 W President Dr #8967, Boise, ID 83713, USA"
        event.description = f"Soccer game at Let's Play Soccer\nField {game['field']}\n{game['home_team']} vs {game['away_team']}"
        # Add reminder 40 minutes before
        event.alarms = [{'trigger': timedelta(minutes=-40)}]
        
        cal.events.add(event)
    
    return cal

def lambda_handler(event, context):
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
                    games, season = scrape_soccer_schedule(team_id)
                    
                    # Add team_id and season to each game for reference
                    for game in games:
                        game['team_id'] = team_id
                        game['season'] = season
                        game['id'] = f"{season}_{game['date']}_{game['home_team']}_{game['away_team']}_{game['field']}"
                        all_games.append(game)
                except Exception as e:
                    failed_teams.append({
                        'team_id': team_id,
                        'error': str(e),
                        'errorType': e.__class__.__name__
                    })
            
            if not all_games and failed_teams:
                return {
                    'statusCode': 404,
                    'headers': {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*'
                    },
                    'body': json.dumps({
                        'error': 'No games found for any of the provided team IDs',
                        'errorType': 'NoGamesFound',
                        'failed_teams': failed_teams,
                        'processed_team_ids': valid_team_ids
                    })
                }
            
            # Sort games by date
            all_games.sort(key=lambda x: datetime.strptime(x['date'], "%a %m/%d %I:%M %p"))
            
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
            
        try:
            calendar = create_calendar_events(games)
            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'calendar': calendar.serialize(),
                })
            }
        except Exception as e:
            return {
                'statusCode': 500,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({'error': str(e)})
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
            games, season = scrape_soccer_schedule(team_id)
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
            calendar = create_calendar_events(games)
            calendar_file = f"{season}_{my_team}_{team_id}.ics"
            
            with open(calendar_file, 'w', newline='\r\n') as f:
                f.write(calendar.serialize())
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

