from curl_cffi import requests
import re
import json
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

def bet365_scraper():
    def start_requests(pd):
        session = requests.Session()
        headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'priority': 'u=1, i',
            'referer': 'https://www.co.bet365.com/?_h=t_3uX6T4-5qJlC5Xiw-SNg%3D%3D&btsffd=1',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
            'x-net-sync-term': '<session token>',
            'x-request-id': '7265b8ed-7121-19c3-7cc9-ef24c9bc8dc6',
        }
        url = 'https://www.co.bet365.com/matchmarketscontentapi/markets'
        params = {
            'lid': '32',
            'zid': '0',
            'pd': (f'#AC#B16#C20525425#D43#{pd}#F43#'),
            'cid': '198',
            'cgid': '3',
            'ctid': '198',
            'csid': '16',
        }
        response = session.get(
            url,
            params=params,
            headers=headers,
        )
        return response.text

    def fraction_to_multiplier(fractional_odds):
        numerator, denominator = map(int, fractional_odds.split('/'))
        multiplier = (numerator / denominator) * 0.8855 + 1
        return round(multiplier, 2)

    def parse(data, prop_name):
        # Lists to store values that are scraped from each match segement
        players, stat_values, prop, over_odds, under_odds = [], [], prop_name, [], []
        # Split the data into segments based on 'SY=fe' (each segement represents one match)
        segments = re.split(r'SY=fe', data) 

        # Iterate through each mach
        for segment in segments[1:]: # first segment is not a match
            # Regular expression to find player names
            name_pattern = re.compile(r"NA=(?!Over|Under)(?![^;]*@)([^;]+);")

            # Find all player names in the segment and append to players list
            player_names = name_pattern.findall(segment)
            if ' ' in player_names:
                player_names.remove(' ') # Remove blank spaces

            # players.update({name: None for name in player_names})
            for name in player_names:
                name = name.replace('  ', ' ') # remove double spacing
                players.append(name)

            # Regular expression to capture the over and under lines
            overPattern = re.compile(r'\|MA;ID=[^;]+;NA=Over.*?(?=\|MA;ID=[^;]+;NA=Under|\|MG|\Z)', re.DOTALL)
            underPattern = re.compile(r'\|MA;ID=[^;]+;NA=Under.*?(?=\|MA|\|MG|\Z)', re.DOTALL)

            # Find the over/under matches
            overMatch = overPattern.search(segment)
            underMatch = underPattern.search(segment)

            # Separate over/under lines
            if overMatch and underMatch:
                # Regular expression to find all over/under lines and odds
                lines_pattern = re.compile(r'HD=([\d\.]+)')
                odds_pattern = re.compile(r'OD=([\d/]+)')

                # Append all over/under odds to respective lists
                stat_values.extend(lines_pattern.findall(overMatch.group(0))) # only add one because it will be the same line
                over_odds.extend(odds_pattern.findall(overMatch.group(0)))
                under_odds.extend(odds_pattern.findall(underMatch.group(0)))
        
        # Convert list of strings to list of floats
        stat_values = [float(i) for i in stat_values]

        # Assign prop name to each prop
        props = [prop] * len(stat_values)
        
        # Convert fractional odds to multiplier
        over_multi_list, under_multi_list = [], []
        for over, under in zip(over_odds, under_odds):
            over_multi_list.append(fraction_to_multiplier(over))
            under_multi_list.append(fraction_to_multiplier(under))

        # Process and return output data
        output_data = [{'player': player, 'prop': prop, 'stat_value': stat, 'over_multi': over_multi, 'under_multi': under_multi} 
                       for player, stat, prop, over_multi, under_multi in zip(players, stat_values, props, over_multi_list, under_multi_list)]
        return output_data

    # List of pds for different props
    pds_map = {
        'E160293': "Strikeouts", 'E160302': "Total Bases", 'E163109': "Hits", 'E160303': "Runs",
        'E160304': "Stolen Bases", 'E160298': "Singles", 'E160299': "Doubles", 'E160300': "Triples",
        'E160297': "Pitching Outs", 'E163108': "Walks Allowed", 'E160296': "Earned Runs Allowed", 'E160295': "Hits Allowed",
        'E163218': "Hits + Runs + RBIs", 'E163219': "Batter Strikeouts"
    }
    output_data = [] # Store final output data
    for pd, prop_name in pds_map.items(): # Iterate through each pd
        response = start_requests(pd)
        output_data.extend(parse(response, prop_name))

    # Export data to json
    output_dir = os.getenv("OUTPUT_DIR")
    with open(os.path.join(output_dir, 'bet365_output.json'), 'w') as f:
        json.dump(output_data, f, indent=2)
    
    # Export data to MySQL
    with connect_to_sql() as (cursor, conn):
        create_table(cursor, 'bet365_data')
        for data in output_data:
            insert_data(cursor, data, 'bet365_data')
        conn.commit()
    
if __name__ == '__main__':
    bet365_scraper()