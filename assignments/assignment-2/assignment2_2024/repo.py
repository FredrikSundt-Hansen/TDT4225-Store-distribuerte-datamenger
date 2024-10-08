from DbConnector import DbConnector
from tabulate import tabulate
import os
from datetime import datetime

def read_labeled_ids(path):
    with open(path, 'r') as file:
        return [line.strip() for line in file if line.strip()]

class Repo:
    

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))
    
    def create_user_table(self):
        query = """
        CREATE TABLE IF NOT EXISTS user (
            id CHAR(3) NOT NULL,
            has_labels BOOLEAN NOT NULL,
            PRIMARY KEY (id)
        )
        """
        self.cursor.execute(query)
        self.db_connection.commit()

    def create_activity_table(self):
        query = """ CREATE TABLE IF NOT EXISTS activity (
                    id BIGINT NOT NULL,
                    user_id CHAR(3) NOT NULL,
                    transportation_mode VARCHAR(10) NULL,
                    start_date_time DATETIME(0) NOT NULL,
                    end_date_time DATETIME(0) NOT NULL,
                    PRIMARY KEY (id),
                    FOREIGN KEY (user_id) REFERENCES user(id) ON DELETE CASCADE ON UPDATE CASCADE)
                """
        self.cursor.execute(query)
        self.db_connection.commit()

    def create_trackpoint_table(self):
        query = """CREATE TABLE IF NOT EXISTS track_point (
                   id INT AUTO_INCREMENT NOT NULL,
                   activity_id BIGINT NOT NULL,
                   lat DOUBLE(9, 6) NOT NULL,
                   lon DOUBLE(9, 6) NULL,
                   altitude INT NOT NULL,
                   date_days DOUBLE(15, 10) NOT NULL,
                   date_time DATETIME(0) NOT NULL,
                   PRIMARY KEY (id),
                   FOREIGN KEY (activity_id) REFERENCES activity(id) ON DELETE CASCADE ON UPDATE CASCADE
                   )
                """
        self.cursor.execute(query)
        self.db_connection.commit()


    def setup_schema(self):
        self.create_user_table()
        self.create_activity_table()
        self.create_trackpoint_table()

    def bulk_insert_users(self, data: list):
        query = "INSERT INTO user (id, has_labels) VALUES (%s, %s)"
        self.cursor.executemany(query, data)
        self.db_connection.commit()

    def bulk_insert_activty(self, data: list):
        query = """
                INSERT INTO activity (
                    id,
                    user_id, 
                    transportation_mode,
                    start_date_time,
                    end_date_time) 
                    VALUES (%s, %s, %s, %s, %s)
                """
        self.cursor.executemany(query, data)
        self.db_connection.commit()

    def bulk_insert_track_point(self, data: list):
        query = """
                INSERT INTO track_point (
                    activity_id, 
                    lat,
                    lon,
                    altitude,
                    date_days,
                    date_time) 
                    VALUES (%s, %s, %s, %s, %s, %s)
                """
        self.cursor.executemany(query, data)
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
            activity_id = None
            start_date_time = None
            end_end_time = None
            transportation_mode = None

            for _, _, files in os.walk(trajectory_path):
                for file in files:
                    activity_id = int(file.split('.')[0])

                    date_time_obj = datetime.strptime(file.split('.')[0], '%Y%m%d%H%M%S')
                    start_date_time = date_time_obj.strftime('%Y-%m-%d %H:%M:%S')

                    file_path = os.path.join(trajectory_path, file)
                    with open(file_path, 'r') as f:
                        lines = f.readlines()[-1].split(',')
                        date_part = lines[-2].strip()  
                        time_part = lines[-1].strip() 
                        end_end_time = f"{date_part} {time_part}"  
            
                    if (start_date_time, end_end_time) in labeled_times:
                        transportation_mode = labeled_times[(start_date_time, end_end_time)]
                    
                    activites_data.append((activity_id, user_id, transportation_mode, start_date_time, end_end_time))
        
        return activites_data
    
    def iter_track_points(self, activites_data):
        track_point_data = []
        for activity_id, user_id, _, _, _ in activites_data:
            plt_path = os.path.join('dataset/Data/', user_id, 'Trajectory', str(activity_id) + ".plt")
            
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


                    track_point_data.append((activity_id, lat, lon, altitude, date_days, date_time))
        
        return track_point_data

    def iter_data(self, limit=1):
        labeled_users = read_labeled_ids('dataset/labeled_ids.txt')
        
        user_data = self.iter_users(limit, labeled_users)
        activites_data = self.iter_activities(user_data, labeled_users)
        track_point_data = self.iter_track_points(activites_data)
        
        print(len(user_data))
        print(len(activites_data))
        print(len(track_point_data))

        #self.insert_dataset(user_data, activites_data, track_point_data)
    

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
        print("ERROR: Failed to use database:", e)
    finally:
        if repo:
            repo.connection.close_connection()

if __name__ == '__main__':
    main()