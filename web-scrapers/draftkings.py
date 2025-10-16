import requests
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import mysql.connector
from contextlib import contextmanager
from collections import defaultdict
import os
from dotenv import load_dotenv
import sys

@contextmanager
def connect_to_sql():
    """Connects to the SQL using contextmanager to efficiently manage the connection and cursor"""
    conn = None
    try:
        # Load the .env file
        load_dotenv()

        # Connect to the MySQL database
        conn = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME")
        )
        cursor = conn.cursor()
        yield cursor, conn  # Yield both cursor and connection to use inside the `with` block
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally: # close cursor and conn after usage
        if cursor:
            cursor.close()
        if conn:
            conn.close() 

def create_table(cursor, table_name):
    """Creates a new table in the MySQL database """
    # Drop the table if it already exists
    drop_table_query = f'DROP TABLE IF EXISTS {table_name}'
    # Create the table
    create_table_query = f'''
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        player VARCHAR(255),
        prop VARCHAR(255),
        stat_value FLOAT,
        over_multi FLOAT,
        under_multi FLOAT
    )
    '''
    cursor.execute(drop_table_query)
    cursor.execute(create_table_query)

def insert_data(cursor, data, table_name):
    """Inserts data into the MySQL database"""
    cursor.execute(
        f'INSERT INTO {table_name} (player, prop, stat_value, over_multi, under_multi) VALUES (%s, %s, %s, %s, %s)',
        (data['player'], data['prop'], data['stat_value'], data['over_multi'], data['under_multi'])
    )

def fraction_to_multiplier(fractional_odds):
    numerator, denominator = map(int, fractional_odds.split('/'))
    multiplier = (numerator / denominator) * 0.924 + 1 # Dilute the odds
    return round(multiplier, 2)

# MLB
def draftkings_mlb_scraper():
    def start_requests(main, sub):
        session = requests.Session()
        headers = {
            'accept': '*/*',
            'origin': 'https://sportsbook.draftkings.com',
            'referer': 'https://sportsbook.draftkings.com/',
            'user-agent': '<user-agent>',
        }
        url = f'https://sportsbook-nash.draftkings.com/api/sportscontent/dkusor/v1/leagues/84240/categories/{main}/subcategories/{sub}'
        response = session.get(
            url,
            headers=headers,
        )
        return response.json()

    def get_ids():
        headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'origin': 'https://sportsbook.draftkings.com',
            'referer': 'https://sportsbook.draftkings.com/',
            'user-agent': '<user-agent>'
        }
        url = 'https://sportsbook-nash.draftkings.com/api/sportscontent/dkusor/v1/leagues/84240/categories/1031/subcategories/6605'
        response = requests.get(
            url,
            headers=headers
        )
        data = response.json()
        subcategories = data['subcategories']
        notNeeded = {684, 517, 1297, 493, 754, 972, 758, 1581, 988}
        ids = [] # (main, sub)
        for sub in subcategories:
            if sub['categoryId'] not in notNeeded:
                ids.append((sub['categoryId'], sub['id']))
        return ids

    def parse(response):        
        # Store the output data
        output_data = []
        # Get the prop name
        propMap = {'Walks (Batter)': 'Batter Walks', 'Strikeouts (Batter)': 'Batter Strikeouts', 'Outs': 'Pitching Outs', 
                    'Alternate Team Total Runs': 'Team Total Runs', 'Strikeouts Thrown': 'Strikeouts',
                    "Run Line - 1st Inning": "1st Inn. Runs Allowed", "Run Line - 2nd Inning": "2nd Inn. Runs Allowed"}
        market = response['markets'][0]
        market_type_name = market['marketType']['name']
        prop_name = market_type_name.replace("O/U", "").strip()
        prop_name = propMap.get(prop_name, prop_name)
        multipliers = defaultdict(list)

        for selection in response['selections']:
            # Get the player name and stat value
            player_name = selection['participants'][0]['name']
            stat_value = selection['points']

            # Get the multipliers for the over and under
            if selection['label'] == "Over":
                over_multi = fraction_to_multiplier(selection['displayOdds']['fractional'])
                multipliers[player_name].append(over_multi)
            elif selection['label'] == "Under":
                under_multi = fraction_to_multiplier(selection['displayOdds']['fractional'])
                multipliers[player_name].append(under_multi)
            
            # Append the base data to the output_data when both multipliers have been found
            if len(multipliers[player_name]) == 2:
                output_data.append({
                    'player': player_name,
                    'prop': prop_name,
                    'stat_value': stat_value,
                    'over_multi': multipliers[player_name][0],
                    'under_multi': multipliers[player_name][1]
                })
        return output_data

    # List of sub categories for different props
    sub_ids = get_ids()
    output_data = []
    # Use ThreadPoolExecutor to send requests concurrently
    with ThreadPoolExecutor(max_workers=10) as executor: 
        futures = [executor.submit(start_requests, main, sub) for main, sub in sub_ids]

        for future in as_completed(futures):
            try:
                response = future.result()
                output_data.extend(parse(response))
            except Exception as e:
                continue

    # Export data to json
    output_dir = os.getenv("OUTPUT_DIR")
    with open(os.path.join(output_dir, 'draftkings_output.json'), 'w') as f:
        json.dump(output_data, f, indent=2)

    # Export data to MySQL
    with connect_to_sql() as (cursor, conn):
        create_table(cursor, 'draftkings_data')
        for data in output_data:
            insert_data(cursor, data, 'draftkings_data')
        conn.commit()

# NBA
def draftkings_nba_scraper():
    def start_requests(main, sub):
        session = requests.Session()
        headers = {
            'accept': '*/*',
            'origin': 'https://sportsbook.draftkings.com',
            'referer': 'https://sportsbook.draftkings.com/',
            'user-agent': '<user-agent>',
        }
        url = f'https://sportsbook-nash.draftkings.com/api/sportscontent/dkusor/v1/leagues/42648/categories/{main}/subcategories/{sub}'
        response = session.get(
            url,
            headers=headers,
        )
        return response.json()

    def get_ids():
        headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'origin': 'https://sportsbook.draftkings.com',
            'referer': 'https://sportsbook.draftkings.com/',
            'user-agent': '<user-agent>'
        }
        url = 'https://sportsbook-nash.draftkings.com/api/sportscontent/dkusor/v1/leagues/42648/categories/1031/subcategories/6605'
        response = requests.get(
            url,
            headers=headers
        )
        data = response.json()
        subcategories = data['subcategories']
        notNeeded = {6230, 14648, 13513, 13513, 6231, 14182, 4609}

        ids = [] # (main, sub)
        for sub in subcategories:
            if sub['categoryId'] not in notNeeded and sub['id'] not in notNeeded:
                ids.append((sub['categoryId'], sub['id']))

        return ids

    def parse(response):        
        # Store the output data
        output_data = []
        # Get the prop name
        propMap = {'Three Pointers Made': '3-Pointers Made', 'Points + Rebounds + Assists': 'Pts + Rebs + Asts'}
        market = response['markets'][0]
        market_type_name = market['marketType']['name']
        prop_name = market_type_name.replace("O/U", "").strip()
        prop_name = propMap.get(prop_name, prop_name)
        prop_name_set.add(prop_name)
        multipliers = defaultdict(list)

        for selection in response['selections']:
            # Get the player name and stat value
            player_name = selection['participants'][0]['name']
            stat_value = selection['points']

            # Get the multipliers for the over and under
            if selection['label'] == "Over":
                over_multi = fraction_to_multiplier(selection['displayOdds']['fractional'])
                multipliers[player_name].append(over_multi)
            elif selection['label'] == "Under":
                under_multi = fraction_to_multiplier(selection['displayOdds']['fractional'])
                multipliers[player_name].append(under_multi)
            
            # Append the base data to the output_data when both multipliers have been found
            if len(multipliers[player_name]) == 2:
                output_data.append({
                    'player': player_name,
                    'prop': prop_name,
                    'stat_value': stat_value,
                    'over_multi': multipliers[player_name][0],
                    'under_multi': multipliers[player_name][1]
                })
        return output_data

    # List of sub categories for different props
    sub_ids = get_ids()
    output_data = []
    prop_name_set = set()
    # Use ThreadPoolExecutor to send requests concurrently
    with ThreadPoolExecutor(max_workers=10) as executor: 
        futures = {executor.submit(start_requests, main, sub): sub for main, sub in sub_ids}

        for future in as_completed(futures):
            try:
                response = future.result()
                parsed_data = parse(response)
                output_data.extend(parsed_data)
            except Exception as e:
                # print(f"Error processing sub {sub}: {e}")
                continue
    # Export data to json
    output_dir = os.getenv("OUTPUT_DIR")
    with open(os.path.join(output_dir, 'draftkings_output.json'), 'w') as f:
        json.dump(output_data, f, indent=2)

    # Export data to MySQL
    with connect_to_sql() as (cursor, conn):
        create_table(cursor, 'draftkings_data')
        for data in output_data:
            insert_data(cursor, data, 'draftkings_data')
        conn.commit()


if __name__ == '__main__':
    draftkings_mlb_scraper()
    draftkings_nba_scraper()