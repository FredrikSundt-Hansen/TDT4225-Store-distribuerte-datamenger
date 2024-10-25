from pymongo import MongoClient, version
import os
from dotenv import load_dotenv




class DbConnector:
    load_dotenv()
    def __init__(self,
                 DATABASE= os.getenv("DATABASE"), 
                 HOST= os.getenv("HOST"),
                 USER= os.getenv("USER"),
                 PASSWORD= os.getenv("PASSWORD")):
        uri = "mongodb://%s:%s@%s/%s" % (USER, PASSWORD, HOST, DATABASE)
        print("URI:", uri)
        # Connect to the databases
        try:
            self.client = MongoClient(uri)
            self.db = self.client[DATABASE]
        except Exception as e:
            raise Exception(f"ERROR: Failed to connect to db: {e}") from e

        # get database information
        print("You are connected to the database:", self.db.name)
        print("-----------------------------------------------\n")

    def close_connection(self):
        # close the cursor
        # close the DB connection
        self.client.close()
        print("\n-----------------------------------------------")
        print("Connection to %s-db is closed" % self.db.name)
