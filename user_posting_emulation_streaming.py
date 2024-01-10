import datetime
import json
import random
import requests
import sqlalchemy
from time import sleep
from sqlalchemy import text
import yaml

# Set seed for reproducibility
random.seed(100)

class AWSDBConnector:
    """
    Class for connecting to the AWS RDS database.
    """
    def __init__(self):
        """
        Initializes the AWSDBConnector with database connection details.
        """
        self.db_creds = self.read_db_creds()
        
    def read_db_creds(self):
        try:
            with open('db_creds.yaml', 'r') as yaml_file:
                db_creds = yaml.safe_load(yaml_file)
                return db_creds
        except FileNotFoundError:
            print("db_creds.yaml file not found. Make sure to create it with the correct credentials.")
            return {}
        
    def create_db_connector(self):
        """
        Creates a SQLAlchemy engine for database connection.

        Returns:
            sqlalchemy.engine.base.Engine: Database engine.
        """
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.db_creds['USER']}:{self.db_creds['PASSWORD']}@{self.db_creds['HOST']}:{self.db_creds['PORT']}/{self.db_creds['DATABASE']}?charset=utf8mb4")
        return engine

class DateTimeEncoder(json.JSONEncoder):
    """
    JSON encoder for datetime objects.
    """
    def default(self, obj):
        """
        Overrides the default method of JSONEncoder to handle datetime objects.

        Args:
            obj (Any): Object to be encoded.

        Returns:
            str: ISO-formatted string for datetime objects.
        """
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()

def run_infinite_post_data_loop():
    """
    Infinitely runs the process of fetching random data from database tables
    and posting it to corresponding AWS Kinesis streams.
    """
    while True:
        # Sleep for random time between 0 and 2 seconds
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:
            # Fetch random rows from database tables
            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            for row in user_selected_row:
                user_result = dict(row._mapping)

            # Update stream names
            pin_stream_name = "streaming-0abb070c336b-pin"
            geo_stream_name = "streaming-0abb070c336b-geo"
            user_stream_name = "streaming-0abb070c336b-user"
            # Construct Kinesis API URLs
            pin_invoke_url = f"https://6kxv09xgo0.execute-api.us-east-1.amazonaws.com/0abb070c336b/streams/{pin_stream_name}/record"
            geo_invoke_url = f"https://6kxv09xgo0.execute-api.us-east-1.amazonaws.com/0abb070c336b/streams/{geo_stream_name}/record"
            user_invoke_url = f"https://6kxv09xgo0.execute-api.us-east-1.amazonaws.com/0abb070c336b/streams/{user_stream_name}/record"
            # Construct payloads for Kinesis API requests
            payload1 = json.dumps({
                "StreamName": pin_stream_name,
                "Data": pin_result,
                "PartitionKey": "index"
            })
            payload2 = json.dumps({
                "StreamName": geo_stream_name,
                "Data": geo_result,
                "PartitionKey": "ind"
            }, cls=DateTimeEncoder)
            payload3 = json.dumps({
                "StreamName": user_stream_name,
                "Data": user_result,
                "PartitionKey": "ind"
            }, cls=DateTimeEncoder)
            headers = {'Content-Type': 'application/json'}
            # Send requests to Kinesis API and print response status codes  
            response1 = requests.request("PUT", pin_invoke_url, headers=headers, data=payload1)
            response2 = requests.request("PUT", geo_invoke_url, headers=headers, data=payload2)
            response3 = requests.request("PUT", user_invoke_url, headers=headers, data=payload3)
            
            print(response1.status_code)
            print(response2.status_code)
            print(response3.status_code)


if __name__ == "__main__":
    # Create an instance of AWSDBConnector
    new_connector = AWSDBConnector()
    # Run the infinite post data loop
    run_infinite_post_data_loop()
    # Print 'Working' for debugging
    print('Working')
