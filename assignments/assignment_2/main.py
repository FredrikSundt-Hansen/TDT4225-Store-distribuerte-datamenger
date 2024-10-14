from geolife_db import GeolifeDB
from geolife_data_handler import process_dataset
from const import ORIGINAL_TRACK_POINT_SIZE, N_USERS, N_ACTIVITES, N_TRACK_POINTS

def main():

    user_data, activity_data, track_point_data = process_dataset(user_limit=200)

    users = len(user_data)
    activities = len(activity_data)
    track_points = len(track_point_data)

    assert users == N_USERS
    assert activities == N_ACTIVITES
    assert track_points == N_TRACK_POINTS

    print(f"Count of users: {users:,}")
    print(f"Count of activities: {activities:,}")
    print(f"Count of track points: {track_points:,}")
    print(f"Cleaned: {ORIGINAL_TRACK_POINT_SIZE - track_points:,} trackpoints\n")

    try:
        with GeolifeDB() as db:
            db.setup_schema()
            print("Schema setup complete")
            db.insert_dataset(user_data, activity_data, track_point_data)
            print("Dataset inserted")
    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == '__main__':
    main()