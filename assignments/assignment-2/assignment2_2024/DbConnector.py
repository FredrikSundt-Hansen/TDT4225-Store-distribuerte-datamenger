import mysql.connector as mysql
from decouple import config

class DatabaseError(Exception):
    """Custom exception for database-related errors"""
    pass

class DbConnector:
    def __init__(self,
                 HOST=config('MYSQL_HOST'),
                 DATABASE_NAME=config('MYSQL_DATABASE'),
                 USER=config('MYSQL_USER'),
                 PASSWORD=config('MYSQL_PASSWORD')):
        try:
            self.db_connection = mysql.connect(host=HOST, database=DATABASE_NAME, user=USER, password=PASSWORD, port=3306)
        except Exception as e:
            raise DatabaseError(f"ERROR: Failed to connect to db: {e}") from e

        # Get the db cursor
        self.cursor = self.db_connection.cursor()

        print("Connected to:", self.db_connection.get_server_info())
        # get database information
        self.cursor.execute("select database();")
        database_name = self.cursor.fetchone()
        print("You are connected to the database:", database_name)
        print("-----------------------------------------------\n")

    def close_connection(self):
        # close the cursor
        self.cursor.close()
        # close the DB connection
        self.db_connection.close()
        print("\n-----------------------------------------------")
        print("Connection to %s is closed" % self.db_connection.get_server_info())
