from DbConnector import DbConnector
from tabulate import tabulate
import os
from datetime import datetime
from config import *

class Repo:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

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


    def setup_schema(self):
        self.create_user_table()
        self.create_activity_table()
        self.create_trackpoint_table()
        self.show_tables()

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

    def iter_users(self, limit, labeled_users):
        users = {}

        i = 0
        for root, _, _ in os.walk('dataset/Data/', topdown=True):
            user_id = root.split('/')[-1]
            
            if not user_id.isdigit():
                continue

            users[user_id] = False

            i += 1
            if i >= limit:
                break

        for user_id in labeled_users:
            if user_id in users:  
                users[user_id] = True  

        return [(user, users[user]) for user in users]

    def iter_activities(self, user_data, labeled_users):
        activites_data = []
        activites_id = 0
        for user_id, _ in user_data:

            labeled_times = {}
            if user_id in labeled_users:
                with open(os.path.join('dataset/Data/', user_id, 'labels.txt')) as f:
                    lines = f.readlines()[1:] 
                    for line in lines:
                        labels_line = line.strip().split('\t')

                        start_date_time = labels_line[0].replace('/', '-')
                        end_end_time = labels_line[1].replace('/', '-')
                        val = labels_line[-1]

                        labeled_times[(start_date_time, end_end_time)] = val

            trajectory_path = os.path.join('dataset/Data/', user_id, 'Trajectory')
            
            start_date_time = None
            end_end_time = None
            transportation_mode = None

            for _, _, files in os.walk(trajectory_path):
                for file in files:
                    file_path = os.path.join(trajectory_path, file)

                    count = 0
                    with open(file_path, 'r') as f:
                        count = len(f.readlines()[6:])

                    if count > 2500:
                        continue

                    date_time_obj = datetime.strptime(file.split('.')[0], '%Y%m%d%H%M%S')
                    start_date_time = date_time_obj.strftime('%Y-%m-%d %H:%M:%S')

                    with open(file_path, 'r') as f:
                        lines = f.readlines()[-1].split(',')
                        date_part = lines[-2].strip()  
                        time_part = lines[-1].strip() 
                        end_end_time = f"{date_part} {time_part}"  
            
                    if (start_date_time, end_end_time) in labeled_times:
                        transportation_mode = labeled_times[(start_date_time, end_end_time)]

                    activites_id += 1
                    activites_data.append((file, user_id, activites_id, transportation_mode, start_date_time, end_end_time))
        
        return activites_data
        
        
    
    def iter_track_points(self, activites_data):
        track_point_data = []
        for file, user_id, activites_id, _, _, _ in activites_data:
            plt_path = os.path.join('dataset/Data/', user_id, 'Trajectory', str(file))
            
            lat = None
            lon = None
            altitude = None
            date_days = None
            date_time = None

    
            with open(plt_path, 'r') as f:
                lines = f.readlines()[6:]
                for line in lines:
                    data = line.split(',')
                    
                    lat = data[0]
                    lon = data[1]
                    altitude = data[3]
                    date_days = data[4]
                    date = data[5]
                    time = data[6]
                    date_time = f"{date} {time}"  

                    track_point_data.append((activites_id, lat, lon, altitude, date_days, date_time))
        
        return track_point_data

    def iter_data(self, limit=200):
        labeled_users = self.read_labeled_ids('dataset/labeled_ids.txt')
        
        user_data = self.iter_users(limit, labeled_users)
        activity_data = self.iter_activities(user_data, labeled_users)
        track_point_data = self.iter_track_points(activity_data)
        
        print(f"Count of users: {len(user_data)}")
        print(f"Count of activities: {len(activity_data)}")
        print(f"Count of track points: {len(track_point_data)}")

        # Remove data that is not for insert
        activity_insert = [(activity_id, user_id, transportation_mode, start_date_time, end_date_time) 
                            for _, user_id, activity_id, transportation_mode, start_date_time, end_date_time in activity_data]
        self.insert_dataset(user_data, activity_insert, track_point_data)
    

    def insert_dataset(self, user_data, activites_data, track_point_data):
        self.bulk_insert_users(user_data)
        self.bulk_insert_activty(activites_data)
        self.bulk_insert_track_point(track_point_data)
  


def main():
    repo = None
    try:
        repo = Repo()
        repo.setup_schema()
        repo.iter_data()
    except Exception as e:
        print("ERROR:", e)
    finally:
        if repo:
            repo.connection.close_connection()

if __name__ == '__main__':
    main()