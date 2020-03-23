# Import dependcies
import os

try:
    from dotenv import load_dotenv
except ImportError:
    from pip._internal import main as pip
    pip(['install', '--user', 'python-dotenv'])
    from dotenv import load_dotenv

from datetime import datetime
from datetime import date
from datetime import time
from datetime import timedelta

try:
    import requests
except ImportError:
    from pip._internal import main as pip
    pip(['install', '--user', 'requests'])
    import requests

try:
    import pandas as pd
except ImportError:
    from pip._internal import main as pip
    pip(['install', '--user', 'pandas'])
    import pandas as pd

try:
    import numpy as np
except ImportError:
    from pip._internal import main as pip
    pip(['install', '--user', 'numpy'])
    import numpy as np
try:
    import sqlalchemy
except ImportError:
    from pip._internal import main as pip
    pip(['install', '--user', 'SQLAlchemy psycopg2-binary'])
    import sqlalchemy

from sqlalchemy import create_engine

# Load env variables
load_dotenv(dotenv_path='/Users/robert/Documents/github/adsbtest/.env')
db = os.getenv("DB")
db_table = os.getenv('DB_TABLE')
json_dump = os.getenv('JSON_DUMP')

# Connect to database
engine = create_engine(db)

# Get today's date
today = date.today()
# Get the current time
time = datetime.now().strftime("%H:%M:%S")
# Create a current time stamp
dateTime = datetime.strptime(datetime.now().strftime(
    '%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S')

# Get JSON dump form dump1090
json_dump = requests.get(json_dump).json()

# Turn JSON dump into a Panda's Data Frame
df1 = pd.json_normalize(json_dump['aircraft'])

# Add a data(date type), time(string type), and time stamp (time stamp type) column.
df1['date'], df1['time'], df1['date_time'] = [today, time, dateTime]

# Remove any rows that do not have locational data
df1.dropna(subset=['lat'], inplace=True)
# Reset the Index for the data frame
df1 = df1.reset_index()

# Only keeping the columns we want
df1 = df1.filter(items=['hex', 'flight', 'alt_baro', 'alt_geom', 'gs', 'track', 'geom_rate', 'squawk',
                        'emergency', 'category', 'nav_qnh', 'nav_altitude_mcp', 'lat', 'lon', 'date', 'time', 'date_time'])

#df2 = pd.read_sql("SELECT * FROM adsbtest WHERE date = current_date AND date_part('hour',time) = date_part('hour',CURRENT_TIMESTAMP)",engine)
# Try to connect to database
try:
    # Only select the last hour of records in the data base and make a data frame of the returned records
    df2 = pd.read_sql(
        "SELECT * FROM adsbtest WHERE date_time >= (NOW() - INTERVAL '1 HOURS')", engine)
    # Set boolen value to True
    dbConnected = True
except:
    # If database does not exist or is unable to connect then print that
    print('Unable to connect to database.')
    # Set boolen value to False
    dbConnected = False

# If the boolen value is set to True then run the below if not then move to add data to database
if dbConnected:
    # Keeping track of how many rows were dropped
    droppedRows = 0
    # For each row in the first data frame do the below
    for index, row in df1.iterrows():
        # If the hex value in the first data frame mataches a hex value in the database data frame then do the below
        if df1['hex'][index] in df2.values:
            # Create a list of of the index location in the database data frame of where the hex value from the first data frame matches
            dflist = df2.index[df2['hex'] == df1['hex'][index]]
            # For each of those indexs in the list do the below
            for item in dflist:
                # Fined the row in the database data frame where the hex values match
                match = df2.loc[[item, ]]
                # Get the time stamp from the first data frame of matching hex values
                df1time = df1['date_time'][index]
                # Get the time stamp from the database data frame of matching hex values
                df2time = match['date_time'][item]
                # Calculate the differences between the two time stamps
                timedif = df1time - df2time
                # If the difference in time is less than 1 hour then do the following
                if timedif.seconds < 3600:
                    # Delete the matching row from the first data frame
                    df1.drop(index, inplace=True)
                    # Incrament the counter of rows dropped
                    droppedRows += 1
                    # End the loop and move on to the next row in the first data frame
                    break
    # Craft a response on how many records were removed
    response = "{} duplicate rows were dropped."
    # Print the response
    print(response.format(droppedRows))

# If All records from the first data frame were removed then print that and exit
if df1.empty:
    print('No new aircraft were added to the database.')
    exit()
else:
    # If there is at least one record in the first data frame then load that into the database and print how many records were loaded before exiting the script.
    df1.to_sql(db_table, con=engine, if_exists='append', index=False)
    response = "{} new aircraft were added to the database."
    print(response.format(len(df1.index)))
    exit()
