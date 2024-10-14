from DbConnector import DbConnector
from tabulate import tabulate
from const import *

class GeolifeDB:
    def __enter__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.connection.close_connection()

    def exec_query(self, query: str):
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    def create_user_table(self):
        query = f"CREATE TABLE IF NOT EXISTS {USER_TABLE_NAME} ({USER_TABLE_SCHEMA})"
        self.cursor.execute(query)
        self.db_connection.commit()

    def create_activity_table(self):
        query = f"CREATE TABLE IF NOT EXISTS {ACTIVITY_TABLE_NAME} ({ACTIVITY_TABLE_SCHEMA})"
        self.cursor.execute(query)
        self.db_connection.commit()

    def create_track_point_table(self):
        query = f"CREATE TABLE IF NOT EXISTS {TRACK_POINT_TABLE_NAME} ({TRACK_POINT_TABLE_SCHEMA})"
        self.cursor.execute(query)
        self.db_connection.commit()

    def drop_tables(self):
        query = f"DROP TABLE IF EXISTS {TRACK_POINT_TABLE_NAME}, {ACTIVITY_TABLE_NAME}, {USER_TABLE_NAME}"
        self.cursor.execute(query)
        self.db_connection.commit()

    def setup_schema(self):
        self.create_user_table()
        self.create_activity_table()
        self.create_track_point_table()

    def bulk_insert_users(self, data: list):
        query = f"INSERT INTO {USER_TABLE_NAME} ({USER_TABLE_INSERT}) VALUES (%s, %s)"
        self.cursor.executemany(query, data)
        self.db_connection.commit()

    def bulk_insert_activity(self, data: list):
        query = f"INSERT INTO {ACTIVITY_TABLE_NAME} ({ACTIVITY_TABLE_INSERT}) VALUES (%s, %s, %s, %s, %s)"
        self.cursor.executemany(query, data)
        self.db_connection.commit()

    """
    One row track_point table = 
    id             activity-id    lat               lon               altitude       date_days         date_time
    INT(4 bytes) + INT(4 bytes) + DOUBLE(8 bytes) + DOUBLE(8 bytes) + INT(4 bytes) + DOUBLE(8 bytes) + DATETIME(8 bytes)
    = 44 bytes
    
    Default max packet size in MySQL = 16 MB

    Batch size = 16 MB / 44 B = 381300
    """
    def bulk_insert_track_point(self, data: list, batch_size: int = int(381300)):
        query = f"INSERT INTO {TRACK_POINT_TABLE_NAME} ({TRACK_POINT_TABLE_INSERT}) VALUES (%s, %s, %s, %s, %s, %s)"
        for i in range(0, len(data), batch_size):
            batch_data = data[i:i + batch_size]  
            self.cursor.executemany(query, batch_data)  
            self.db_connection.commit()  

    def insert_dataset(self, user_data, activity_data, track_point_data):
        self.bulk_insert_users(user_data)
        self.bulk_insert_activity(activity_data)
        self.bulk_insert_track_point(track_point_data)