from bs4 import BeautifulSoup
import requests
from ics import Calendar, Event
from datetime import datetime, timedelta, timezone
import re

def scrape_soccer_schedule(team_id):
    
    # URL of the schedule page
    url = f"https://www.letsplaysoccer.com/4/teamSchedule/{team_id}"
    
    # Fetch the webpage
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
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

def create_calendar_events(games):
    cal = Calendar()
    mt_offset = -7
    tz = timezone(timedelta(hours=mt_offset))
    
    # Add calendar metadata
    cal.creator = 'Soccer Schedule Scraper'
    
    for game in games:
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
    team_ids = event.get('team_ids', [])
    results = {
        'processed': [],
        'failed': [],
        'calendars': {}
    }
    
    for team_id in team_ids:
        try:
            games, season = scrape_soccer_schedule(team_id)
            
            # Count team appearances to find my_team
            team_counts = {}
            for game in games:
                team_counts[game['home_team']] = team_counts.get(game['home_team'], 0) + 1
                team_counts[game['away_team']] = team_counts.get(game['away_team'], 0) + 1
            
            my_team = max(team_counts.items(), key=lambda x: x[1])[0]
            calendar = create_calendar_events(games)
            
            results['calendars'][team_id] = {
                'season': season,
                'team': my_team,
                'calendar_data': calendar.serialize()
            }
            results['processed'].append(team_id)
            
        except Exception as e:
            results['failed'].append({'team_id': team_id, 'error': str(e)})
    
    return results

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

