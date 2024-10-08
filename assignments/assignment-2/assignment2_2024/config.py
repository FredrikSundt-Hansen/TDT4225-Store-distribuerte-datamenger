USER_TABLE_NAME = "user"
USER_TABLE_SCHEMA = "id CHAR(3) NOT NULL, has_labels BOOLEAN NOT NULL, PRIMARY KEY (id)"
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