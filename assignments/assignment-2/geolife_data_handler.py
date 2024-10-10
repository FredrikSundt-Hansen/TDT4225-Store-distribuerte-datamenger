import os
from const import *

def is_valid_user_id(user_id: str) -> bool:
    return user_id.isdigit()


def has_too_many_track_points(count: int) -> bool:
    return count > MAX_TRACK_POINTS


def read_labeled_ids(path: str) -> list:
    with open(path, 'r') as file:
        return [line.strip() for line in file if line.strip()]


def is_valid_altitude(altitude) -> bool:
    return float(altitude) != -777.0


def process_labeled_timestamp(labeled_timestamp: dict, file_path) -> None:
    with open(file_path) as f:
        for line in f.readlines()[LABELED_ID_FILE_HEADER_SIZE:]:
            line = line.strip().split('\t')
            start_date_time = line[0].replace('/', '-')
            end_end_time = line[1].replace('/', '-')
            val = line[-1]
            labeled_timestamp[(start_date_time, end_end_time)] = val


def process_activity_data(activity_data: list, activity_id: int, user_id: str,
                          labeled_timestamp: dict, first_line: str, last_line: str) -> None:
    first_line = first_line.strip().split(',')
    date, time = first_line[-2], first_line[-1]
    start_date_time = f"{date} {time}"

    last_line = last_line.strip().split(',')
    date, time = last_line[-2], last_line[-1]
    end_date_time = f"{date} {time}"

    transportation_mode = labeled_timestamp.get((start_date_time, end_date_time), None)
    activity_data.append((activity_id, user_id, transportation_mode, start_date_time, end_date_time))


def process_track_point_data(track_point_data: list, activity_id: int, lines: list[str]) -> None:
    for line in lines:
        lat, lon, _, altitude, date_days, date, time = line.split(',')
        date_time = f"{date} {time}"

        if not is_valid_altitude(altitude):
            altitude = None

        track_point_data.append((activity_id, lat, lon, altitude, date_days, date_time))


def process_dataset(user_limit: int) -> tuple:
    user_data, activity_data, track_point_data = [], [], []
    labeled_users = read_labeled_ids(LABELED_ID_PATH)

    activity_id, i = 0, 0
    for root, _, _ in os.walk(DATASET_PATH, topdown=True):
        user_id = root.split('/')[-1]
        if not is_valid_user_id(user_id):
            continue

        user_data.append((user_id, user_id in labeled_users))

        labeled_timestamp = {}
        if user_id in labeled_users:
            process_labeled_timestamp(labeled_timestamp, os.path.join(root, USER_LABEL_FILE))

        data_files_path = os.path.join(root, DATA_FILES_DIR)
        for _, _, files in os.walk(data_files_path):
            for file in files:
                with open(os.path.join(data_files_path, file), 'r') as f:
                    lines = f.readlines()[USER_DATA_HEADER_SIZE:]
                    if has_too_many_track_points(len(lines)):
                        continue

                    activity_id += 1
                    first_line = lines[0]
                    last_line = lines[-1]
                    process_activity_data(activity_data, activity_id, user_id, labeled_timestamp, first_line, last_line)
                    process_track_point_data(track_point_data, activity_id, lines)

        i += 1
        if i >= user_limit:
            break

    return user_data, activity_data, track_point_data
