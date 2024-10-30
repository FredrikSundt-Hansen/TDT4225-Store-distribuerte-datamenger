import os
from const import *
from datetime import datetime

def is_valid_user_id(user_id: str) -> bool:
    return user_id.isdigit()

def has_too_many_track_points(count: int) -> bool:
    return count > MAX_TRACK_POINTS

def read_labeled_ids(path: str) -> list:
    with open(path, 'r') as file:
        return [line.strip() for line in file if line.strip()]

def is_valid_altitude(altitude_str) -> bool:
    return float(altitude_str) != -777.0

def extract_date_time(date_str: str, time_str) -> datetime | None:
    date_str = date_str.replace('/', '-')
    date_time_str = f"{date_str} {time_str}"
    try:
        date_time_obj = datetime.strptime(date_time_str, DATE_TIME_FORMAT)
    except ValueError as e:
        print(f"Error parsing date and time '{date_time_str}': {e}")
        return None

    return date_time_obj

def extract_date_time_from_line(line):
    line_parts = line.strip().split(',')
    date_str, time_str = line_parts[-2], line_parts[-1]
    return extract_date_time(date_str, time_str)

def process_labeled_timestamp(labeled_timestamp: dict, file_path) -> None:
    with open(file_path) as f:
        for line in f.readlines()[LABELED_ID_FILE_HEADER_SIZE:]:
            line_parts = line.strip().split('\t')
            start_date_str = line_parts[0].replace('/', '-')
            end_date_str = line_parts[1].replace('/', '-')
            transportation_mode = line_parts[2]
            try:
                start_date_time = datetime.strptime(start_date_str, DATE_TIME_FORMAT)
                end_date_time = datetime.strptime(end_date_str, DATE_TIME_FORMAT)
            except ValueError as e:
                print(f"Error parsing labeled dates '{start_date_str}' or '{end_date_str}': {e}")
                continue
            labeled_timestamp[(start_date_time, end_date_time)] = transportation_mode

def process_activity_data(activity_data, activity_id, user_id, labeled_timestamp, first_line, last_line):
    start_date_time = extract_date_time_from_line(first_line)
    end_date_time = extract_date_time_from_line(last_line)
    transportation_mode = labeled_timestamp.get((start_date_time, end_date_time), None)
    activity_data.append((activity_id, user_id, transportation_mode, start_date_time, end_date_time))

def process_track_point_data(track_point_data: list, activity_id: str, lines: list[str]) -> None:
    for line in lines:
        lat_str, lon_str, _, altitude_str, date_days_str, date_str, time_str = line.strip().split(',')
        lat = float(lat_str)
        lon = float(lon_str)
        altitude = float(altitude_str) if is_valid_altitude(altitude_str) else None
        date_days = float(date_days_str)
        date_time = extract_date_time(date_str, time_str)
        track_point_data.append((activity_id, lat, lon, altitude, date_days, date_time))


def process_dataset(user_limit: int = None) -> tuple:
    user_data, activity_data, track_point_data = [], [], []
    labeled_users = set(read_labeled_ids(LABELED_ID_PATH))

    user_count = 0
    for root, dirs, files in os.walk(DATASET_PATH, topdown=True):
        user_id = os.path.basename(root)
        if not is_valid_user_id(user_id):
            continue

        has_labels = user_id in labeled_users
        user_data.append((user_id, has_labels))

        labeled_timestamp = {}
        if has_labels:
            label_file_path = os.path.join(root, USER_LABEL_FILE)
            if os.path.exists(label_file_path):
                process_labeled_timestamp(labeled_timestamp, label_file_path)

        data_files_path = os.path.join(root, DATA_FILES_DIR)
        if not os.path.exists(data_files_path):
            continue

        activity_counter = 0
        for data_file in os.listdir(data_files_path):
            file_path = os.path.join(data_files_path, data_file)
            if not data_file.endswith('.plt'):
                continue
            with open(file_path, 'r') as f:
                lines = f.readlines()[USER_DATA_HEADER_SIZE:]
                if has_too_many_track_points(len(lines)):
                    continue

                first_line = lines[0]
                last_line = lines[-1]
                activity_id = f"{user_id}_{activity_counter}"
                activity_counter += 1
                process_activity_data(activity_data, activity_id, user_id, labeled_timestamp, first_line, last_line)
                process_track_point_data(track_point_data, activity_id, lines)

        user_count += 1
        if user_limit and user_count >= user_limit:
            break

    return user_data, activity_data, track_point_data