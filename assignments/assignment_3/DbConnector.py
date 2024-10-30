from pymongo import MongoClient
import os
from dotenv import load_dotenv

class DbConnector:
    load_dotenv()

    def __init__(self,
                 DATABASE=os.getenv("DB_NAME"),
                 HOST=os.getenv("DB_HOST"),
                 USER=os.getenv("DB_USER"),
                 PASSWORD=os.getenv("DB_PASSWORD")):
        uri = "mongodb://%s:%s@%s:27017/%s" % (USER, PASSWORD, HOST, DATABASE)

        # Connect to the databases
        try:
            self.client = MongoClient(uri)
            self.db = self.client[DATABASE]
        except Exception as e:
            print("ERROR: Failed to connect to db:", e)

        # get database information
        print("Connected to the database:", self.db.name)
        print("-----------------------------------------------\n")

    def close_connection(self):
        # close the cursor
        # close the DB connection
        self.client.close()
        print("\n-----------------------------------------------")
        print("Connection to %s-db is closed" % self.db.name)
