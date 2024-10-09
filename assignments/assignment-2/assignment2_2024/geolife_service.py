import os
from config import *

from typing import NamedTuple

class TrackPoint(NamedTuple):
    activity_id: int
    lat: float
    lon: float
    altitude: float
    date_days: float
    date_time: str

class GeolifeService:
    def __init__(self) -> None:
        pass

    def is_valid_user_id(self, user_id: str) -> bool:
        return user_id.isdigit()

    def has_too_many_track_points(self, lines: list) -> bool:
        return len(lines) > MAX_TRACK_POINTS

    def is_invalid_altitude(self, track_point_line: list) -> bool:
        return float(track_point_line[ALTITUDE_INDEX]) == -777.0

    def read_labeled_ids(self, path: str) -> list:
        with open(path, 'r') as file:
            return [line.strip() for line in file if line.strip()]

    def process_labeled_timestamp(self, labeled_timestamp: dict, file_path) -> None:
        with open(file_path) as f:
            for line in f.readlines()[LABELED_ID_FILE_HEADER_SIZE:]:
                line = line.strip().split('\t')
                start_date_time = line[0].replace('/', '-')
                end_end_time = line[1].replace('/', '-')
                val = line[-1]
                labeled_timestamp[(start_date_time, end_end_time)] = val
        

    def process_activity_data(self, activity_data: list, activity_id: int, user_id: str, labeled_timestamp: dict, file_name: str, last_line: str) -> None:
        file_name = file_name.split('.')[0]
        start_date_time = file_name[0:4] + "-" + file_name[4:6] + "-" + file_name[6:8] + " " + file_name[8:10] + ":" + file_name[10:12] + ":" + file_name[12:14]

        last_line = last_line.strip().split(',')

        date, time = last_line[-2], last_line[-1]
        end_date_time = f"{date} {time}"

        transportation_mode = labeled_timestamp.get((start_date_time, end_date_time), None)
        activity_data.append((activity_id, user_id, transportation_mode, start_date_time, end_date_time))

    def process_track_point_data(self, track_point_data: list, activity_id: int, line: list) -> None:
        lat, lon, _, altitude, date_days, date, time = line
        date_time = f"{date} {time}"
        track_point_data.append((activity_id, lat, lon, altitude, date_days, date_time))

    def process_user_activity(self, file_path: str, file_name, user_id: str, activity_id: int, labeled_timestamp: dict, activity_data: list, track_point_data: list) -> None:
        with open(file_path, 'r') as f:
            lines = f.readlines()[USER_DATA_HEADER_SIZE:]
            if self.has_too_many_track_points(lines):
                return

            self.process_activity_data(activity_data, activity_id, user_id, labeled_timestamp, file_name, lines[-1])
            for line in lines:
                line = line.split(',')
                if self.is_invalid_altitude(line):
                    return
                
                self.process_track_point_data(track_point_data, activity_id, line)

        return activity_id
        
    def process_dataset(self, limit: int) -> tuple:
        user_data, activity_data, track_point_data = [], [], []
        labeled_users = self.read_labeled_ids(LABELED_ID_PATH)

        activity_id, i = 0, 0
        for root, _, _ in os.walk(DATASET_PATH, topdown=True):
            user_id = root.split('/')[-1]
            if not self.is_valid_user_id(user_id):
                continue

            user_data.append((user_id, user_id in labeled_users))

            labeled_timestamp = {}
            if user_id in labeled_users:
                self.process_labeled_timestamp(labeled_timestamp, os.path.join(root, USER_LABEL_FILE))

            data_files_path = os.path.join(root, DATA_FILES_DIR)
            for _, _, files in os.walk(data_files_path):
                for file in files:
                    activity_id += 1
                    self.process_user_activity(os.path.join(data_files_path, file), file, user_id, activity_id, labeled_timestamp, activity_data, track_point_data)

            i += 1
            if i >= limit:
                break
        
        return user_data, activity_data, track_point_data