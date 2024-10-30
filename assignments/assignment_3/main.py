import traceback

from geolife_data_handler import process_dataset
from geolife_db import GeolifeDB
from const import ORIGINAL_TRACK_POINT_SIZE, N_USERS, N_ACTIVITIES, N_TRACK_POINTS

def main():
    print("Processing dataset...\n")
    user_data, activity_data, track_point_data = process_dataset(user_limit=200)

    users = len(user_data)
    activities = len(activity_data)
    track_points = len(track_point_data)

    assert users == N_USERS
    assert activities == N_ACTIVITIES
    assert track_points == N_TRACK_POINTS

    print(f"Count of users: {users:,}")
    print(f"Count of activities: {activities:,}")
    print(f"Count of track points: {track_points:,}")
    print(f"Cleaned: {ORIGINAL_TRACK_POINT_SIZE - track_points:,} track points\n")

    try:
        with GeolifeDB() as db:
            print("Creating collections...\n")
            db.create_collections()
            db.show_collections()

            print("\nClearing all collections...\n")
            db.clear_all_collections()

            print("Inserting dataset into the database...\n")
            db.insert_dataset(user_data, activity_data, track_point_data)
            print("Insertion complete.\n")
    except Exception as e:
        print("ERROR:", e)
        traceback.print_exc()

if __name__ == '__main__':
    main()