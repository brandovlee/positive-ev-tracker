import scrapy
import json
import requests
import unicodedata
import math
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
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

class UnderdogScraper(scrapy.Spider):
    name = 'underdog'
    allowed_domains = ['api.underdogfantasy.com']
    start_urls = ['https://api.underdogfantasy.com/beta/v6/over_under_lines']

    def parse(self, response):
        data = json.loads(response.body)
        lines = data.get("over_under_lines", [])
        prop_names = {
            "FANTASY", "POINTS", "REBOUNDS", "ASSISTS", "STEALS", "BLOCKS", "TURNOVERS",
            "1ST", "INN.", "STRIKEOUTS", "OUTS", "RUNS", "HITS", "WALKS", "BASES",
            "RBI", "HOME", "TOTAL", "SINGLES", "DOUBLES", "TRIPLES", "HOMERUNS", 
            "RBIS", "HR", "GAMES", "AC", "GAME", "SET", "MATCH", "ACES", "DOUBLES", 
            "FAULTS", "SETS", "GAMES", "LOST", "WON", "SERVES", "RETURN", "PITCHES", 
            "RUNS ALLOWED", "PITCHING", "OUTS", "GOALS", "RUSHING", "1-3", "EARNED",
            "BATTER", "1H", "DOUBLE", "KICKING", "3PM", "TACKLES", "PASSING", "RUSH",
            "RECEIVING", "COMPLETIONS", "INTERCEPTIONS", "FG", "XP", "LONGEST", "KICKING",
            "KILLS", "DEATHS", "HEADSHOTS", "RECEPTIONS", "SACKS", "PASSES", "CROSSES",
            "PTS", "3-POINTERS", "STROKES", "BIRDIES", "TOP", "BOGEYS", "SAVES", "SHOTS",
            "FINISHING", "CLEARANCES", "FOULS", "PASS"
        }
        output_data = []
        unique = set()
        for line in lines:
            stat_value = float(line.get('stat_value'))
            words = line['over_under']['title'].split() 
            payout_multipliers = []
            for option in line.get('options'):
                multiplier = float(option['payout_multiplier']) * math.sqrt(3.15) # noramlize to 1.77 
                payout_multipliers.append(round(multiplier, 2))

            player_name = None
            prop_name = None

            for i, word in enumerate(words):
                if word.upper() in prop_names:
                    player_name = ' '.join(words[:i])
                    prop_name = ' '.join(words[i:])
                    break
            
            prop_name = prop_name.replace("O/U", "").strip() if prop_name else "UNKNOWN" 
            unique.add(prop_name)
            if len(payout_multipliers) > 1:
                output_data.append({
                    'player': player_name if player_name else prop_name,
                    'prop': prop_name,
                    'stat_value': stat_value,
                    'over_multi': payout_multipliers[0],
                    'under_multi': payout_multipliers[1]
                })

        # Export data to JSON
        output_dir = os.getenv("OUTPUT_DIR")
        with open(os.path.join(output_dir, 'underdog_output.json'), 'w') as f:
            json.dump(output_data, f, indent=2)
        
        # Export data to MySQL
        with connect_to_sql() as (cursor, conn):
            create_table(cursor, 'underdog_data')
            for data in output_data:
                insert_data(cursor, data, 'underdog_data')
            conn.commit()

class VividPicksScraper(scrapy.Spider):
    name = 'vividpicks'
    allowed_domains = ['api.betcha.one']
    start_urls = ['https://api.betcha.one/v1/game/activePlayersForLeagueBoard']

    def start_requests(self):
        headers = {
            "Accept-Language": "en-us",
            "Authorization": "<auth token>",
            "Cache-Control": "no-cache",
            "Content-Type": "application/json",
            "User-Agent": "VividPicks/230 CFNetwork/1498.700.2 Darwin/23.6.0",
            "Cookie": "<cookie data>"
        }
        payload = {
            "league": "Multi-Sport",
            "matchUp": False
        }
        for url in self.start_urls:
            yield scrapy.Request(
                url = url,
                method = "POST",
                headers = headers,
                body = json.dumps(payload),
                callback=self.parse
            )

    def parse(self, response):
        data = json.loads(response.body)
        games = data.get("gret", [])
        propMap = {
            "TotalBases": "Total Bases", "RunsBattedIn": "Runs Batted In", "PitchingStrikeouts": "Pitching Outs",
            "ReceivingYards": "Receiving Yards", "RushingYards": "Rushing Yards", "PassingYards": "Passing Yards", 
            "ReceivingTouchdowns": "Receiving TDs", "Receiving Tds": "Receiving TDs", "Pts + Ast": "Points + Assists",
            "3PT Made": "3-Pointers Made", "Shots OnTarget": "Shots on Target", "Points + Rebounds + Assists": "Pts + Rebs + Asts",
            "Pts + Reb": "Points + Rebounds", "Interceptions Thrown": "Interceptions", "Field Goals Made": "FG Made",
            "Shot Attempts": "Shots Attempted", "Passing Tds": "Passing TDs", "RushingTouchdowns": "Rushing TDs",
            "Reb + Ast": "Rebounds + Assists", "Rushing Touchdowns": "Rushing TDs", "PassingTouchdowns": "Passing TDs",
            "Total Tackles": "Tackles", "Kills Gm 1-3": "kills in game 1+3", "Earned Runs": "Earned Runs Allowed",
            "Pass Attempts": "Passing Attempts", "Rush Attempts": "Rushing Attempts"
        }
        output_data = []
        for game in games:
            active_players = game.get("activePlayers")

            for player in active_players:
                player_name = player.get("name")
                normalized_name = unicodedata.normalize('NFKD', player_name).encode('ascii', 'ignore').decode('ascii')
                visual_props = player.get("visiblePlayerProps", {})
                config_props = player.get("configPlayerProps", {})

                for prop in visual_props:
                    prop_name = propMap.get(prop.get("p"), prop.get("p"))
                    prop_value = prop.get("val")
                    config_prop = config_props.get(prop_name, None)
                    multiplier = config_prop.get("multiplier") if config_prop else 1
                    # Skip if the multiplier isn't 1
                    if multiplier != 1:
                        continue

                    output_data.append({
                        'player': normalized_name,
                        'prop': prop_name,
                        'stat_value': prop_value,
                        'over_multi': 1.77,
                        'under_multi': 1.77
                    })

        output_dir = os.getenv("OUTPUT_DIR")
        with open(os.path.join(output_dir, 'vividpicks_output.json'), 'w') as f:
            json.dump(output_data, f, indent=2)

        # Export data to MySQL
        with connect_to_sql() as (cursor, conn):
            create_table(cursor, 'vividpicks_data')
            for data in output_data:
                insert_data(cursor, data, 'vividpicks_data')
            conn.commit()

class SleeperScraper(scrapy.Spider):
    name = 'sleeper'
    allowed_domains = ['api.sleeper.app']
    start_urls = ['https://api.sleeper.app/lines/available?dynamic=true&include_preseason=true&first_sport=nfl,nba,mlb,wnba,nhl,cfb,cbb']
    start_urls = ['https://api.sleeper.app/lines/available?dynamic=true&include_preseason=true&first_sport=nba'] # use nba only
    # start_urls = ['https://api.sleeper.app/lines/available?dynamic=true&include_preseason=true&first_sport=mlb'] # use mlb only
    # start_urls = ['https://api.sleeper.app/lines/available?dynamic=true&include_preseason=true&first_sport=cbb'] # use cbb only

    def start_requests(self):
        headers = {
            'x-amp-session':'1724883144450',
            'accept':'application/json',
            'authorization':'<auth token>',
            'x-api-client':'api.cached',
            'accept-language':'en-US,en;q=0.9',
            'if-none-match':'W/"8e1234e2bc0925f3b1271dc12f91f6f4"',
            'user-agent':'<user-agent>',
            'x-device-id':'<device id>',
            'x-platform':'ios',
            'x-build':'93.2.v4258',
            'x-bundle':'com.blitzstudios.sleeperbot'
        }
        for url in self.start_urls:
            yield scrapy.Request(
                url = url,
                method = "GET",
                headers = headers,
                callback=self.parse
            )

    # Creates a hashmap to map the subject id to the player's name
    def get_mlb_player_map(self): 
        response_data = requests.get("https://api.sleeper.app/v1/players/mlb")
        players_data = response_data.json()

        players_map = {}

        for player_id, player_info in players_data.items():
            metadata = player_info.get("metadata", {})
            full_name = metadata.get("full_name", "UNKNOWN")
            normalized_name = unicodedata.normalize('NFKD', full_name).encode('ascii', 'ignore').decode('ascii') # convert special characters
            players_map[player_id] = normalized_name

        return players_map
    def get_nba_player_map(self): 
        response_data = requests.get("https://api.sleeper.app/v1/players/nba")
        players_data = response_data.json()

        players_map = {}

        for player_id, player_info in players_data.items():
            full_name = player_info.get("full_name", "UNKNOWN")
            normalized_name = unicodedata.normalize('NFKD', full_name).encode('ascii', 'ignore').decode('ascii') # convert special characters
            players_map[player_id] = normalized_name

        return players_map
    def get_cbb_player_map(self): 
        response_data = requests.get("https://api.sleeper.app/v1/players/cbb")
        players_data = response_data.json()

        players_map = {}

        for player_id, player_info in players_data.items():
            full_name = player_info.get("full_name", "UNKNOWN")
            normalized_name = unicodedata.normalize('NFKD', full_name).encode('ascii', 'ignore').decode('ascii')
            players_map[player_id] = normalized_name
        return players_map

    def parse(self, response):
        data = json.loads(response.body)
        output_data = []
        # playerMap = self.get_mlb_player_map()
        # propMap = {
        #     "fantasy_points": "Fantasy Points", "hits_runs_rbis": "Hits + Runs + RBIs",
        #     "strike_outs": "Strikeouts", "doubles": "Doubles", "hits_allowed": "Hits Allowed",
        #     "bat_strike_outs": "Batter Strikeouts", "earned_runs": "Earned Runs Allowed",
        #     "first_inning_runs": "1st Inn. Runs", "hits": "Hits", "runs": "Runs",
        #     "walks": "Batter Walks", "rbis": "RBIs", "home_runs": "Home Runs",
        #     "singles": "Singles", "total_bases": "Total Bases", "outs": "Pitching Outs",
        #     "stolen_bases": "Stolen Bases", "bat_walks": "Batter Walks",
        #     "1st Inn. Runs": "1st Inn. Runs Allowed"
        # }
        playerMap = self.get_nba_player_map()
        propMap = {
            "fantasy_points": "Fantasy Points", "blocks": "Blocks", "steals": "Steals", "assists": "Assists", "points": "Points",
            "rebounds": "Rebounds", "turnovers": "Turnovers", "threes_made": "3-Pointers Made", "points_rebounds": "Points + Rebounds",
            "points_and_assists": "Points + Assists", "rebounds_and_assists": "Rebounds + Assists", "pts_reb_ast": "Pts + Rebs + Asts",
            "blocks_and_steals": "Blocks + Steals", "points_and_rebounds": "Points + Rebounds", "points_and_assists": "Points + Assists",
        }

        for item in data:
            options = item["options"]
            subjectid = options[0]["subject_id"]
            player_name = playerMap.get(subjectid, subjectid)
            prop_name = propMap.get(options[0]["wager_type"], options[0]["wager_type"])
            stat_value = options[0]["outcome_value"]
            payout_multipliers = []

            for option in options:
                payout_multipliers.append(float(option["payout_multiplier"]))

            output_data.append({
                'player': player_name,
                'prop': prop_name,
                'stat_value': stat_value,
                'over_multi': payout_multipliers[0],
                'under_multi': payout_multipliers[1]
            })
        output_dir = os.getenv("OUTPUT_DIR")
        with open(os.path.join(output_dir, 'sleeper_output.json'), 'w') as f:
            json.dump(output_data, f, indent=2)

        # Export data to MySQL
        with connect_to_sql() as (cursor, conn):
            create_table(cursor, 'sleeper_data')
            for data in output_data:
                insert_data(cursor, data, 'sleeper_data')
            conn.commit()

def main():
    settings = get_project_settings()
    process = CrawlerProcess(settings)
    process.crawl(UnderdogScraper)
    process.crawl(VividPicksScraper)
    process.crawl(SleeperScraper)
    process.start()

if __name__ == '__main__':
    main()