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

        self.cursor = self.db_connection.cursor()
        self.cursor.execute("select database();")
        database_name = self.cursor.fetchone()
        print("-----------------------------------------------\n")
        print("Connected to:", self.db_connection.get_server_info())
        print("Connected to the database:", database_name)
        

    def close_connection(self):
        self.cursor.close()
        self.db_connection.close()
        print(f"Database connection closed")
        print("\n-----------------------------------------------")
