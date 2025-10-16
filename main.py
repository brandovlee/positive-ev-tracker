import pandas as pd
import requests
import pytz
import mysql.connector
import os
from dotenv import load_dotenv
from datetime import datetime
from contextlib import contextmanager
from collections import defaultdict

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
    create_table_query = f'''
    CREATE TABLE IF NOT EXISTS {table_name}_results (
        id INT AUTO_INCREMENT PRIMARY KEY,
        player VARCHAR(255),
        prop VARCHAR(255),
        stat_value FLOAT,
        `o/u` VARCHAR(10),
        top_sb VARCHAR(255),
        top_multi FLOAT,
        low_sb VARCHAR(255),
        low_multi FLOAT,
        spread FLOAT,
        avg_multi FLOAT
    )
    '''
    cursor.execute(create_table_query)

def manage_database(df, cursor, conn, table_name):
    """Manage the database by removing, updating, and inserting props."""
    # Retrieve existing props from the database
    cursor.execute(f"SELECT player, prop, stat_value, `o/u` FROM {table_name}_results")
    existing_props = cursor.fetchall()
    existing_props_set = set(existing_props)

    # Convert DataFrame to a set of tuples for comparison
    current_props_set = set(df[['player', 'prop', 'stat_value', 'o/u']].itertuples(index=False, name=None))

    # Find props to remove, update, and insert
    props_to_remove = existing_props_set - current_props_set
    props_to_insert = current_props_set - existing_props_set
    props_to_update = current_props_set & existing_props_set

    # Remove props that aren't available anymore
    for prop in props_to_remove:
        cursor.execute(f"DELETE FROM {table_name}_results WHERE player=%s AND prop=%s AND stat_value=%s AND `o/u`=%s", prop)

    # Update player props that had value changes
    for prop in props_to_update:
        row = df[(df['player'] == prop[0]) & (df['prop'] == prop[1]) & (df['stat_value'] == prop[2]) & (df['o/u'] == prop[3])].iloc[0]
        cursor.execute(f"""
            UPDATE {table_name}_results
            SET top_sb=%s, top_multi=%s, low_sb=%s, low_multi=%s, spread=%s, avg_multi=%s
            WHERE player=%s AND prop=%s AND stat_value=%s AND `o/u`=%s
        """, (row['top_sb'], row['top_multi'], row['low_sb'], row['low_multi'], row['spread'], row['avg_multi'], row['player'], row['prop'], row['stat_value'], row['o/u']))

    # Insert new props that weren't in the pre-existing database
    new_props = []
    for prop in props_to_insert:
        row = df[(df['player'] == prop[0]) & (df['prop'] == prop[1]) & (df['stat_value'] == prop[2]) & (df['o/u'] == prop[3])].iloc[0]
        cursor.execute(f"""
            INSERT INTO {table_name}_results (player, prop, stat_value, `o/u`, top_sb, top_multi, low_sb, low_multi, spread, avg_multi)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (row['player'], row['prop'], row['stat_value'], row['o/u'], row['top_sb'], row['top_multi'], row['low_sb'], row['low_multi'], row['spread'], row['avg_multi']))
        new_props.append(row)

    # Commit the changes
    conn.commit()

    return pd.DataFrame(new_props)

def load_data_from_db(table_name):
    """Load data from MySQL database."""
    with connect_to_sql() as (cursor, conn):
        # Fetch column names excluding 'id'
        cursor.execute(f"SHOW COLUMNS FROM {table_name}")
        columns = [col[0] for col in cursor.fetchall() if col[0] != 'id']
        
        # Select only the columns excluding 'id'
        query = f"SELECT {', '.join(columns)} FROM {table_name}"
        cursor.execute(query)
        data = cursor.fetchall()
        
        return pd.DataFrame(data, columns=columns)

def merge_dataframes(merged_df, sportsbook):
    """Merge two DataFrames on 'player', 'prop', and 'stat_value'."""
    return pd.merge(merged_df, sportsbook, on=["player", "prop", "stat_value"], how='outer')

def find_greatest_difference(row, sportsbooks):
    """Finds the greatest difference in multiplers between sportsbooks in a row"""
    differences = {}
    dfs_apps = {'vividpicks', 'parlayplay', 'sleeper', 'prizepicks', 'underdog'}
    
    # Add eligible props to the differences map
    for sportbook in sportsbooks:
        multipliers = row[sportbook]
        if isinstance(multipliers, (list, tuple)) and len(multipliers) == 2:
            differences[sportbook] = (multipliers[0], multipliers[1])
    
    # Initialize variables to track the minimum and maximum values along with their associated sportsbook
    worstOver, bestOver = float("inf"), float("-inf")
    worstUnder, bestUnder = float("inf"), float("-inf")
    worstOverSportbook, bestOverSportbook = None, None

    # Find the sportsbook with the maximum difference and append to list to find avg
    overList, underList = [], []
    for sportbook, (overMultiplier, underMultiplier) in differences.items():
        # Append to over/under list
        overList.append(overMultiplier), underList.append(underMultiplier)

        # Potentially update best value lines
        if sportbook in dfs_apps: # only find EV+ bets in dfs apps
            if overMultiplier >= bestOver:
                bestOver = overMultiplier
                bestOverSportbook = sportbook
            if underMultiplier >= bestUnder:
                bestUnder = underMultiplier
                bestUnderSportbook = sportbook

        # Potentially update worst value lines
        if overMultiplier <= worstOver:
            worstOver = overMultiplier
            worstOverSportbook = sportbook
        if underMultiplier <= worstUnder:
            worstUnder = underMultiplier
            worstUnderSportbook = sportbook
    
    # Return None if the best value isn't connected to a dfs app
    if not bestOverSportbook:
        return None

    # Find the over, under differences
    overDiff, underDiff = round(abs(worstOver - bestOver), 2), round(abs(worstUnder - bestUnder), 2)

    # Find the average of the list exclusing the outliers
    overList.remove(max(overList))
    underList.remove(max(underList))
    if len(overList) > 1:
        overList.remove(min(overList))
    if len(underList) > 1:        
        underList.remove(min(underList))
    overAvg = round(sum(overList) / len(overList), 2)
    underAvg = round(sum(underList) / len(underList), 2)

    overResult = ["O", bestOverSportbook, bestOver, worstOverSportbook, worstOver, overDiff, overAvg]
    underResult = ["U", bestUnderSportbook, bestUnder, worstUnderSportbook, worstUnder, underDiff, underAvg]

    return [overResult, underResult]

def apply_find_greatest_difference(df, sportsbooks):
    """Apply the find_greatest_difference() function onto the dataframe"""
    # Apply the greatest difference function and separate the lists
    results = df.apply(find_greatest_difference, sportsbooks=sportsbooks, axis=1)

    # Filter out None results
    valid_results = results[results.notnull()]
    
    # Split the valid_results into 'over' and 'under' DataFrames
    over_results = valid_results.apply(lambda x: x[0])
    under_results = valid_results.apply(lambda x: x[1])

    # Create DataFrame columns from the results
    temp1 = df.loc[valid_results.index].copy()
    temp2 = df.loc[valid_results.index].copy()
    temp1[['o/u', 'top_sb', 'top_multi', 'low_sb', 'low_multi', 'spread', 'avg_multi']] = pd.DataFrame(over_results.tolist(), index=temp1.index)
    temp2[['o/u', 'top_sb', 'top_multi', 'low_sb', 'low_multi', 'spread', 'avg_multi']] = pd.DataFrame(under_results.tolist(), index=temp2.index)
    combined_df = pd.merge(temp1, temp2, how='outer', on=['player', 'prop', 'stat_value', 'o/u', 'top_sb', 
                                                          'top_multi', 'low_sb', 'low_multi', 'spread', 'avg_multi'])
    
    # Remove the sportsbooks cols containing the multipliers as they are not needed at this point
    combined_df = combined_df[['player', 'prop', 'stat_value', 'o/u', 'top_sb', 'top_multi', 'low_sb', 
                            'low_multi', 'spread', 'avg_multi']]
    
    return combined_df

def apply_filters(df):
    # Remove low differences
    filtered_df = (df[df['top_multi'] - df['avg_multi'] >= 0.05]
    .query("1.7 <= top_multi <= 1.78") # Specify the threshold of values
    .query("avg_multi <= 1.68")
    .sort_values(by='avg_multi')) # Sorting the values in ascending order

    return filtered_df

def save_all_props_to_csv(temp, sportsbooks):
    """Saves all the props to a csv to make it easier to read and anaylze"""
    # Convert back to american odds for readability
    if 'bet365' in sportsbooks:
        temp['bet365'] = temp.apply(lambda row: decimal_to_american('bet365', row), axis=1)

    if 'draftkings' in sportsbooks:
        temp['draftkings'] = temp.apply(lambda row: decimal_to_american('draftkings', row), axis=1)

    # Export to csv
    output_dir = os.getenv("OUTPUT_DIR")
    save_to_csv(temp, os.path.join(output_dir, 'all_disc.csv'))

def save_to_csv(df, filepath):
    """Save the DataFrame to a CSV file."""
    df.to_csv(filepath, index=False)

def identify_unique_props(dataframes, sportsbooks):
    """Identify unique prop names that are only present in one sportsbook."""
    prop_sets = {sportbook: set(df['prop']) for sportbook, df in dataframes.items()}
    all_props = set.union(*prop_sets.values())
    unique_props = {prop: [sportbook for sportbook in sportsbooks if prop in prop_sets[sportbook]] for prop in all_props}
    unique_props = {prop: sportbook_list for prop, sportbook_list in unique_props.items() if len(sportbook_list) == 1}
    
    # Display unique props and their corresponding sportsbook
    for prop, sportbook_list in unique_props.items():
        if sportbook_list[0] != "underdog":
            print(f"Prop: {prop} is only present in: {sportbook_list[0]}")

def decimal_to_american(sportbook, row):
    """Convert decimal odds to American odds."""
    decimal_odds = row[sportbook]

    # Skip if nan
    if not decimal_odds or isinstance(decimal_odds, float):
        return None

    # Reverse diluted odds
    if sportbook == "bet365":
        decimal_odds = [(((decimal_odds[0] - 1) / 0.8855) + 1), (((decimal_odds[1] - 1) / 0.8855) + 1)]
    elif sportbook == "draftkings":
        decimal_odds = [(((decimal_odds[0] - 1) / 0.924) + 1), (((decimal_odds[1] - 1) / 0.924) + 1)]

    # Convert each element of the decimal_odds list to American odds
    american_odds = []
    for odds in decimal_odds:
        if odds == 1: # prevent float division by zero
            american_odds.append(0)
            continue
        if odds >= 2.0:
            new_odds = (odds - 1) * 100
            rounded_odds = round(new_odds / 5) * 5
            american_odds.append(rounded_odds)
        else:
            new_odds = -100 / (odds - 1)
            rounded_odds = round(new_odds / 5) * 5
            american_odds.append(rounded_odds)
        
    return american_odds

def retrieve_prop_info(all_props, new_props, sportsbooks):
    '''Finds all the matching rows between two dataframes'''
    # Initialize an empty DataFrame to collect matching rows
    matching_rows = pd.DataFrame()

    # Iterate through each row in new_props
    for _, row in new_props.iterrows():
        matching_row = all_props[
            (all_props['player'] == row['player']) &
            (all_props['prop'] == row['prop']) &
            (all_props['stat_value'] == row['stat_value'])
        ]

        # Get the over or under line depending
        if not matching_row.empty:
            for book in sportsbooks:
                if book in matching_row.columns:
                    if row['o/u'] == 'O':
                        matching_row.loc[:, book] = matching_row[book].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else None)
                    elif row['o/u'] == 'U':
                        matching_row.loc[:, book] = matching_row[book].apply(lambda x: x[1] if isinstance(x, list) and len(x) > 1 else None)

            # Append the modified row to the results DataFrame
            matching_rows = pd.concat([matching_rows, matching_row])

    return matching_rows

def send_discord_webhook(df, sportsbook_odds):
    """Sends a discord webhook alert to a specific url"""
    url = os.getenv("DISCORD_WEBHOOK_URL")

    # Define image mapping
    imageMap = {
        'underdog': "https://betsperts-wp-live.s3.us-east-2.amazonaws.com/new-site/wp-content/uploads/2022/06/14163519/underdog-logo-small-1.jpg",
        'prizepicks': "https://images.prismic.io/colormatics/ee35bee7-9d3e-41f7-9b96-905305d14a12_Prizepicks+P.png?auto=format%2Ccompress&fit=max&q=50&w=800",
        'vividpicks': "https://mld9m31eb7ss.i.optimole.com/w:240/h:240/q:mauto/f:avif/https://www.oddsshopper.com/wp-content/uploads/2023/10/vivid-picks-logo.png",
        'sleeper': "https://sleepercdn.com/landing/web2021/img/sleeper-app-logo-2.png",
        "parlayplay": "https://play-lh.googleusercontent.com/Rj-2vqkn-cyRq5_KpPwmkbD8F_kCo6xufs-kdMch8e7mBScHwfyBMTeK0_Op2ObxvQ=w240-h480-rw",
    }

    # Convert df to dict
    rows = df.to_dict(orient="records")

    # Determine the sportsbook
    sportsbook = rows[0]['top_sb']
    description = ""
    
    for row in rows:
        description += (
            f"**{row['player']} {row['o/u']} {row['stat_value']} {row['prop']}**\n"
            f"Top SB: {row['top_sb']} @ {row['top_multi']} | "
            f"Low SB: {row['low_sb']} @ {row['low_multi']} | "
            f"Avg Multi: {row['avg_multi']}\n"
        )

        prop_key = (row['player'], row['prop'], row['stat_value'])
        if prop_key in sportsbook_odds:
            description += "**All Sportsbook Odds:**\n"
            for book, odds in sportsbook_odds[prop_key]:
                description += f"{book}: {odds}\n"
        description += "\n"
    
    # Get the current time in PST
    now = datetime.now(pytz.timezone('America/Los_Angeles'))

    # Build the Discord webhook payload
    data = {
        "embeds": [{
            "title": f"**{sportsbook.capitalize()} Odds Alert**",
            "description": description,
            "thumbnail": {
                "url": imageMap[sportsbook]
            },
            "footer": {
                "text": "Odds provided by brandovlee"
            },
            "timestamp": now.isoformat()
        }]
    }
    
    requests.post(url, json=data)

def main():
    # List of sportsbooks
    sportsbooks = ['draftkings', 'vividpicks', 'parlayplay', 'sleeper', 'prizepicks', 'underdog']
    bookies = {'draftkings'}

    # Dictionary to store DataFrames
    dataframes = {}

    # Load data for each sportbook and clean it
    for sportbook in sportsbooks.copy():
        # Remove suffixes from the sportbook name
        sportbook_cleaned = sportbook.replace(' Jr', '').replace(' II', '').strip()
        table_name = (f'{sportbook_cleaned}_data')
        sportbook_df = load_data_from_db(table_name)

        if sportbook_df.empty: # Remove if empty dataframe
            sportsbooks.remove(sportbook)
        else:
            # Merge over and under multipliers into payout_multipliers list
            sportbook_df[sportbook] = sportbook_df[['over_multi', 'under_multi']].values.tolist()
            sportbook_df.drop(columns=['over_multi', 'under_multi'], inplace=True)
            dataframes[sportbook] = sportbook_df

    # Merge the dataframes
    merged_df = dataframes[sportsbooks[0]]
    for sportbook in sportsbooks[1:]:
        merged_df = merge_dataframes(merged_df, dataframes[sportbook])

    # Remove props that is only available unless its on at least 2 sportsbooks
    merged_df.dropna(thresh=6, inplace=True)

    # Create a temp copy and then store all props to use for later usage
    all_props = merged_df.copy()
    save_all_props_to_csv(all_props, sportsbooks)

    # Apply filters and find discrepancies
    calculated_df = apply_find_greatest_difference(merged_df, sportsbooks)
    filtered_df = apply_filters(calculated_df)

    # Seperate data by top_sb
    dfs_df = {}
    grouped = filtered_df.groupby('top_sb') # groups the filtered_df by top_sb
    for sportbook, group in grouped:
        if sportbook not in bookies:
            dfs_df[sportbook] = group
    
    # Use the Context Manager for database operations
    with connect_to_sql() as (cursor, conn):
        for sportbook, df in dfs_df.items():
            create_table(cursor, sportbook)
            new_props = manage_database(df, cursor, conn, sportbook)
            conn.commit()

            # Only send discord alert if there are new props
            if not new_props.empty:
                # Retrieve all odds given a list of props
                matching_props = retrieve_prop_info(all_props, new_props, sportsbooks)
                            
                # Create a dictionary to store odds from all sportsbooks
                sportsbook_odds = defaultdict(list)  # [prop, (sportbook, odds)]
                for prop in matching_props.itertuples(index=False):
                    prop_key = (prop.player, prop.prop, prop.stat_value) 
                    for book in sportsbooks:
                        if not pd.isnull(getattr(prop, book)):
                            sportsbook_odds[prop_key].append((book, getattr(prop, book)))

                # Sort the new props by avg_multi
                sorted_new_props = new_props.sort_values(by='avg_multi')

                # Send discord alert
                send_discord_webhook(sorted_new_props, sportsbook_odds)
                output_dir = os.getenv("OUTPUT_DIR")
                save_to_csv(filtered_df, os.path.join(output_dir, 'sorted_filtered_discrepancies.csv'))

if __name__ == "__main__":
    main()