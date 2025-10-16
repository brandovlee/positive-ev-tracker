from curl_cffi import requests
import json
import unicodedata
import mysql.connector
from contextlib import contextmanager
import os
from dotenv import load_dotenv

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

def scrape_prizepicks():
    headers = {
        'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
        'X-Device-Info': 'name=,os=mac,osVersion=10.15.7,isSimulator=false,platform=web,appVersion=web,fbp=fb.1.1723660011058.49143379871310946',
        'sec-ch-ua-mobile': '?0',
        'User-Agent': '<user agent>',
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Referer': 'https://app.prizepicks.com/',
        'X-Device-ID': '<device id>',
        'sec-ch-ua-platform': '"macOS"',
    }
    # mlb
    # params = {
    #     'league_id': '2',
    #     'per_page': '1000',
    #     'single_stat': 'true',
    #     'state_code': 'OR',
    #     'game_mode': 'prizepools',
    # }
    # nba
    params = {
        'league_id': '7',
        'per_page': '1000',
        'single_stat': 'true',
        'state_code': 'OR',
        'game_mode': 'prizepools',
    }
    url = 'https://api.prizepicks.com/projections'
    response = requests.get(url, params=params, headers=headers)
    output_data = []

    try:
        data = response.json()
        newPlayers = data['included']
        projections = data['data']
        
        propMap = {"Hitter Fantasy Score": "Fantasy Points", "Hits+Runs+RBIs": "Hits + Runs + RBIs", "Pitcher Strikeouts": "Strikeouts",
                "Pitcher Fantasy Score": "Fantasy Points", "Hitter Strikeouts": "Batter Strikeouts", "Pts+Rebs+Asts": "Pts + Rebs + Asts",
                "Pts+Asts": "Points + Assists", "Pts+Rebs": "Points + Rebounds", "Rebs+Asts": "Rebounds + Assists", "Blks+Stls": "Blocks + Steals", 
                "3-PT Made": "3-Pointers Made",}
        for projection in projections:
            # Extract the player ID
            player_id = projection['relationships']['new_player']['data']['id']
            
            # Extract the player's name (using the 'new_player' relationship)
            player_name = None
            for player in newPlayers:
                if player['id'] == player_id and player['type'] == 'new_player':
                    player_name = player['attributes']['name']
                    normalized_name = unicodedata.normalize('NFKD', player_name).encode('ascii', 'ignore').decode('ascii')
                    break
            
            # Extract and append player prop details if standard multiplier
            if projection['attributes']['odds_type'] == 'standard':
                stat_type = projection['attributes']['stat_type']
                prop_name = propMap.get(stat_type, stat_type)
                line_score = projection['attributes']['line_score']
                output_data.append({
                    'player': normalized_name,
                    'prop': prop_name,
                    'stat_value': line_score,
                    'over_multi': 1.77,
                    'under_multi': 1.77
                })
    except Exception as e:
        print("Error scraping PrizePicks data: ", e)

    # Save the JSON response to an output file
    output_dir = os.getenv("OUTPUT_DIR")
    output_file = os.path.join(output_dir, 'prizepicks_output.json')
    with open(output_file, 'w') as f:
        json.dump(output_data, f, indent=2)

    # Export data to MySQL
    with connect_to_sql() as (cursor, conn):
        create_table(cursor, 'prizepicks_data')
        for data in output_data:
            insert_data(cursor, data, 'prizepicks_data')
        conn.commit()

def scrape_parlayplay():
    headers = {
        'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
        'X-ParlayPlay-Platform': 'web',
        'X-Parlay-Request': '1',
        'sec-ch-ua-mobile': '?0',
        'User-Agent': '<user agent>',
        'Accept': 'application/json, text/plain, */*',
        'Referer': 'https://parlayplay.io/challenges/crossgame/search',
        'X-Requested-With': 'XMLHttpRequest',
        'X-CSRFToken': '<csrf token>',
        'sec-ch-ua-platform': '"macOS"',
    }

    params = {
        'sport': 'All',
        'league': '',
        'includeAlt': 'true',
    }
    url = 'https://parlayplay.io/api/v1/crossgame/search/'
    response = requests.get(url, params=params, headers=headers)
    data = response.json()
    players = data['players']
    output_data = []
    propMap = {
    'Fantasy Score': 'Fantasy Points', 'Batting Walks': 'Batter Walks', 'Outs': 'Pitching Outs',
    'Batting Strikeouts': 'Batter Strikeouts', 'Walks': 'Walks Allowed', 'Bases': 'Total Bases', 
    '1st 2 Maps Kills': '1st 2 Maps Player Kills', 'Made Threes': '3-Pointers Made', 
    'Points + Rebounds + Assists': 'Pts + Rebs + Asts', "Earned Runs": "Earned Runs Allowed",
    "1st 2 Maps  Kills": "Kills on Map 1+2", "Passing Completions": "Completions",
    "Longest Passing Completion": "Longest Completion", "Passing Touchdowns": "Passing TDs"}

    for player in players:
        player_name = player['player']['fullName']
        normalized_name = unicodedata.normalize('NFKD', player_name).encode('ascii', 'ignore').decode('ascii')
        for stat in player['stats']:
            altLines = stat.get('altLines', None)
            if altLines:
                values = altLines['values']
                for value in values:
                    # Get prop name
                    prop_name = value['marketName']
                    prop_name = prop_name.replace('Player', '').strip() # remove "Player' prefix
                    stat_value = value['selectionPoints']
                    under_multiplier = value['decimalPriceUnder']
                    over_multiplier = value['decimalPriceOver']

                    # Skip if there are no multipliers for either over/unders
                    if not under_multiplier or not over_multiplier:
                        continue

                    output_data.append({
                        'player': normalized_name,
                        'prop': propMap.get(prop_name, prop_name),
                        'stat_value': stat_value,
                        'over_multi': over_multiplier,
                        'under_multi': under_multiplier
                    })
    # Save the JSON response to an output file
    output_dir = os.getenv("OUTPUT_DIR")
    output_file = os.path.join(output_dir, 'parlayplay_output.json')
    with open(output_file, 'w') as f:
        json.dump(output_data, f, indent=2)

    # Export data to MySQL
    with connect_to_sql() as (cursor, conn):
        create_table(cursor, 'parlayplay_data')
        for data in output_data:
            insert_data(cursor, data, 'parlayplay_data')
        conn.commit()

if __name__ == '__main__':
    #scrape_parlayplay()
    scrape_prizepicks()