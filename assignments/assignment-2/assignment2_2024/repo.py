from DbConnector import DbConnector
from tabulate import tabulate
import os
from datetime import datetime
from config import *
import traceback

class Repo:
    def __enter__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.cursor.close()
        self.db_connection.close()

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    def read_labeled_ids(self, path):
        with open(path, 'r') as file:
            return [line.strip() for line in file if line.strip()]
    
    def create_user_table(self):
        query = f"CREATE TABLE IF NOT EXISTS {USER_TABLE_NAME} ({USER_TABLE_SCHEMA})"
        self.cursor.execute(query)
        self.db_connection.commit()

    def create_activity_table(self):
        query = f"CREATE TABLE IF NOT EXISTS {ACTIVITY_TABLE_NAME} ({ACTIVITY_TABLE_SCHEMA})"
        self.cursor.execute(query)
        self.db_connection.commit()

    def create_trackpoint_table(self):
        query = f"CREATE TABLE IF NOT EXISTS {TRACK_POINT_TABLE_NAME} ({TRACK_POINT_TABLE_SCHEMA})"
        self.cursor.execute(query)
        self.db_connection.commit()

    def delete_from_user_table(self):
        query = f"DELETE FROM {USER_TABLE_NAME}"
        self.cursor.execute(query)
        self.db_connection.commit()

    def setup_schema(self):
        self.create_user_table()
        self.create_activity_table()
        self.create_trackpoint_table()

    def bulk_insert_users(self, data: list):
        query = f"INSERT INTO {USER_TABLE_NAME} ({USER_TABLE_INSERT}) VALUES (%s, %s)"
        self.cursor.executemany(query, data)
        self.db_connection.commit()

    def bulk_insert_activty(self, data: list):
        query = f"INSERT INTO {ACTIVITY_TABLE_NAME} ({ACTIVITY_TABLE_INSERT}) VALUES (%s, %s, %s, %s, %s)"
        self.cursor.executemany(query, data)
        self.db_connection.commit()

    # One row is 48 bytes, MYSQL default max packet size = 16 MB
    # 16 MB / 48 bytes = 349525, thus 349525 / 7 batch size
    def bulk_insert_track_point(self, data: list, batch_size: int = int(349525 / 7)):
        query = f"INSERT INTO {TRACK_POINT_TABLE_NAME} ({TRACK_POINT_TABLE_INSERT}) VALUES (%s, %s, %s, %s, %s, %s)"
        for i in range(0, len(data), batch_size):
            batch_data = data[i:i + batch_size]  
            self.cursor.executemany(query, batch_data)  
            self.db_connection.commit()  
    
    def process_labeled_timestamp(self, labeled_timestamp, line: str):
        start_date_time = line[0].replace('/', '-')
        end_end_time = line[1].replace('/', '-')
            
        val = line[-1]

        labeled_timestamp[(start_date_time, end_end_time)] = val
        
    def process_activity_data(self, activity_data: list, activity_id: int, user_id: str, labeled_timestamp: dict, file_name: str, last_line: str):
        date_time_obj = datetime.strptime(file_name.split('.')[0], '%Y%m%d%H%M%S')
        start_date_time = date_time_obj.strftime('%Y-%m-%d %H:%M:%S')

        date_part = last_line[-2].strip()  
        time_part = last_line[-1].strip() 
        end_date_time = f"{date_part} {time_part}"  

        transportation_mode = None
        if (start_date_time, end_date_time) in labeled_timestamp:
            transportation_mode = labeled_timestamp[(start_date_time, end_date_time)]

        activity_data.append((activity_id, user_id, transportation_mode, start_date_time, end_date_time))
    
    def process_track_point_data(self, track_point_data: list, activity_id: int, line: list[str]):
        lat = line[0]
        lon = line[1]
        altitude = line[3]
        date_days = line[4]
        date = line[5]
        time = line[6]
        date_time = f"{date} {time}" 

        track_point_data.append((activity_id, lat, lon, altitude, date_days, date_time))
    
    def iter_dataset(self, limit):
        user_data = []
        activity_data = []
        track_point_data = []

        labeled_users = self.read_labeled_ids(LABELED_ID_PATH)
        
        activity_id = 0
        i = 0
        for root, _, _ in os.walk(DATASET_PATH, topdown=True):
            user_id = root.split('/')[-1]
            if not user_id.isdigit():
                continue

            user_data.append((user_id, user_id in labeled_users))

            labeled_timestamp = {}
            if user_id in labeled_users:
                with open(os.path.join(DATASET_PATH, user_id, USER_LABEL_FILE)) as f:
                    lines = f.readlines()[LABELED_ID_FILE_HEADER_SIZE:]
                    for line in lines:
                        line = line.strip().split('\t')
                        self.process_labeled_timestamp(labeled_timestamp, line)

            for _, _, files in os.walk(os.path.join(DATASET_PATH, user_id, DATA_FILES_DIR)):
                for file in files:
                    file_path = os.path.join(DATASET_PATH, user_id, DATA_FILES_DIR, file)
                    with open(file_path, 'r') as f:
                        lines = f.readlines()[USER_DATA_HEADER_SIZE:]
                        if len(lines) > MAX_TRACK_POINTS:
                            continue
                        
                        activity_id += 1
                        last_line = lines[-1].split(',')
                        self.process_activity_data(activity_data, activity_id, user_id, labeled_timestamp, file, last_line)

                        for line in lines:
                            line = line.split(',')
                            if float(line[3]) == -777.0:
                                continue
                            
                            self.process_track_point_data(track_point_data, activity_id, line)
        
            i += 1
            if i >= limit:
                break
        
        return user_data, activity_data, track_point_data

    def process_dataset(self, limit=200):
        user_data, activity_data, track_point_data = self.iter_dataset(limit)
        
        print(f"Count of users: {len(user_data):,}")
        print(f"Count of activities: {len(activity_data):,}")
        print(f"Count of track points: {len(track_point_data):,}")
        print(f"Cleaned: {ORIGINAL_TRACK_POINT_SIZE - len(track_point_data):,} trackpoints")

        self.insert_dataset(user_data, activity_data, track_point_data)

    def insert_dataset(self, user_data, activity_data, track_point_data):
        self.bulk_insert_users(user_data)
        self.bulk_insert_activty(activity_data)
        self.bulk_insert_track_point(track_point_data)
        

def main():
    repo = None
    try:
        with Repo() as repo:
            repo.setup_schema()
            repo.process_dataset()
    except Exception as e:
        print(f"ERROR: {e}")
        traceback.print_exc() 
    finally:
        if repo:
            repo.connection.close_connection()

if __name__ == '__main__':
    main()