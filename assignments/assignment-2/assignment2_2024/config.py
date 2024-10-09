USER_TABLE_NAME = "user"
USER_TABLE_SCHEMA = "id CHAR(3) NOT NULL, has_labels BOOLEAN NOT NULL, PRIMARY KEY (id)"
USER_TABLE_INSERT = "id, has_labels"

ACTIVITY_TABLE_NAME = "activity"
ACTIVITY_TABLE_SCHEMA = """
    id BIGINT NOT NULL,
    user_id CHAR(3) NOT NULL,
    transportation_mode VARCHAR(10) NULL,
    start_date_time DATETIME(0) NOT NULL,
    end_date_time DATETIME(0) NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (user_id) REFERENCES user(id) ON DELETE CASCADE ON UPDATE CASCADE
"""
ACTIVITY_TABLE_INSERT = "id, user_id, transportation_mode, start_date_time, end_date_time"

TRACK_POINT_TABLE_NAME = "track_point"
TRACK_POINT_TABLE_SCHEMA = """
    id INT AUTO_INCREMENT NOT NULL,
    activity_id BIGINT NOT NULL,
    lat DOUBLE(9, 6) NOT NULL,
    lon DOUBLE(9, 6) NULL,
    altitude INT NOT NULL,
    date_days DOUBLE(15, 10) NOT NULL,
    date_time DATETIME(0) NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (activity_id) REFERENCES activity(id) ON DELETE CASCADE ON UPDATE CASCADE
"""
# Leave out 'id' since it's auto-incremented
TRACK_POINT_TABLE_INSERT = "activity_id, lat, lon, altitude, date_days, date_time"

ORIGINAL_TRACK_POINT_SIZE = 24427363

DATASET_PATH = "dataset/Data/"
LABELED_ID_PATH = "dataset/labeled_ids.txt"
USER_LABEL_FILE = "labels.txt"
DATA_FILES_DIR = "Trajectory"
LABELED_ID_FILE_HEADER_SIZE = 1
USER_DATA_HEADER_SIZE = 6

MAX_TRACK_POINTS = 2500
ALTITUDE_INDEX = 3
