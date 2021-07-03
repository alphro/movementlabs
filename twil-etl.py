#necessary libraries
from datetime import datetime, timedelta 
from twilio.rest import Client
import pandas as pd
import psycopg2
from psycopg2 import OperationalError

############################# Retrieving the Data from Twilio ##########################

#adjustment for days
day_pass = 5 #day_pass determines range of days since today for recieved messages
             #EX: day_pass = 4 means retrieve all messages within the last 4 days
today = datetime.today()
prime_date=today-timedelta(days=day_pass)

#account_sid and auth_token for connecting with twilio server
account_sid = "XXXXXX"
auth_token = "XXXXXX"

#client authentication  
client = Client(account_sid, auth_token)

#retrieving messages from this specific account_sid
messages = client.messages \
                 .list(
                      date_sent_after=prime_date
                  )

#retrives all needed information for messages regarding this sid 
for record in messages:
    print(record.sid)

############################# Twilio Data to Redshift ##########################

#function that creates connection to progresql server
def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        print("Connection to PostgreSQL DB successful")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return connection

#function for creating a new database
def create_database(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Query executed successfully")
    except OperationalError as e:
        print(f"The error '{e}' occurred")

#function that allows for executing data queries
def execute_query(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Query executed successfully")
    except OperationalError as e:
        print(f"The error '{e}' occurred")

#connecting to main postgresql default database
connection = create_connection(
    "postgres", "postgres", "postgres", "localhost", "5432"
)

#creating new database twilio_etl, only need to do once
# create_database_query = "CREATE DATABASE twilio_etl"
# create_database(connection, create_database_query)

#connection to new twilio_etl database
connection= create_connection(
    "twilio_etl", "postgres", "postgres", "localhost", "5432"
)

#SQL CREATE TABLE to match Schema with Postgres
create_twilio2RedShift_table = """
 CREATE TABLE IF NOT EXISTS twilio2RedShift (
    sid Varchar(34),
     error_code integer,
     error_message Varchar(255),
     Date_created Varchar(31),
     Date_sent Varchar(31),
     Date_updated Varchar(31),
     Status Varchar(20)
)
"""

#creates the table for the database twilio-etl
execute_query(connection, create_twilio2RedShift_table)

######IMPORTANT#####
#from above for loop, still have to do, replace with others
lists = [
    ("James", 25, "male", "USA"),
    ("Leila", 32, "female", "France"),
    ("Brigitte", 35, "female", "England"),
    ("Mike", 40, "male", "Denmark"),
    ("Elizabeth", 21, "female", "Canada"),
]

#formatting for postgres
lists_records = ", ".join(["%s"] * len(lists))   

#inserting all information into postgres
insert_query = (
    f"INSERT INTO twilio2RedShift (sid, error_code, error_message, Date_created, Date_sent, Date_updated, Status) VALUES {lists_records}"
)

#commits the changes
connection.autocommit = True

#creates a cursor to the connection
cursor = connection.cursor()

#executes the connection 
cursor.execute(insert_query, lists)

#closes the connection to ensure no leaking 
connection.close() 
