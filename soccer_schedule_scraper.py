from bs4 import BeautifulSoup
import requests
from ics import Calendar, Event
from datetime import datetime, timedelta, timezone
import re
import json

def validate_team_id(team_id: str) -> bool:
    """Validate that a team ID is properly formatted."""
    if not isinstance(team_id, str):
        return False
    if not re.match(r'^\d{6}$', team_id):
        return False
    # Ensure it's a positive integer when parsed
    try:
        num_id = int(team_id)
        return num_id > 0
    except ValueError:
        return False

def scrape_soccer_schedule(team_id):
    # Validate team ID before making request
    if not validate_team_id(team_id):
        raise ValueError(f"Invalid team ID format: {team_id}")
    
    # URL of the schedule page
    url = f"https://www.letsplaysoccer.com/4/teamSchedule/{team_id}"
    
    # Fetch the webpage
    try:
        response = requests.get(url, timeout=10)  # Add timeout
        response.raise_for_status()  # Raise exception for bad status codes
    except requests.RequestException as e:
        raise RuntimeError(f"Failed to fetch schedule for team {team_id}: {str(e)}")
    
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Check if page contains valid team data
    if "Team not found" in response.text or "Invalid team" in response.text:
        raise ValueError(f"Team ID {team_id} not found or invalid")
    
    # Find season number with simplified search
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
        print(f"Error extracting season: {e}")
        SEASON = "Unknown"

    # Find all table rows
    games = []
    rows = soup.find_all('tr')
    
    for row in rows:
        cells = row.find_all('td')
        if len(cells) == 5:  # Verify it's a game row
            date_time = cells[0].text.strip()
            field = cells[1].text.strip().split(' ')[1]  # Extract just the number
            home_team = cells[2].find('span').text.strip()
            away_team = cells[3].find('span').text.strip()
            
            game = {
                'date': date_time,
                'field': field,
                'home_team': home_team,
                'away_team': away_team
            }
            games.append(game)
    
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
                'body': json.dumps({'error': 'team_ids is required'})
            }
        
        # Split and clean team IDs
        team_ids = [tid.strip() for tid in team_ids_param.split(',') if tid.strip()]
        
        # Validate and deduplicate team IDs
        valid_team_ids = []
        invalid_team_ids = []
        seen_ids = set()
        
        for team_id in team_ids:
            if team_id in seen_ids:
                continue
            if validate_team_id(team_id):
                valid_team_ids.append(team_id)
                seen_ids.add(team_id)
            else:
                invalid_team_ids.append(team_id)
        
        if not valid_team_ids:
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'error': 'No valid team IDs provided',
                    'invalid_ids': invalid_team_ids
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
                    failed_teams.append({'team_id': team_id, 'error': str(e)})
            
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
                    'error': str(e),
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

