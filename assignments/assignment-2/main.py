from geolife_db import GeolifeDB
from geolife_data_handler import GeolifeDataHandler 
from const import ORIGINAL_TRACK_POINT_SIZE

def main():
    data_handler = GeolifeDataHandler()
    user_data, activity_data, track_point_data = data_handler.process_dataset(user_limit=200)

    print(f"Count of users: {len(user_data):,}")
    print(f"Count of activities: {len(activity_data):,}")
    print(f"Count of track points: {len(track_point_data):,}")
    print(f"Cleaned: {ORIGINAL_TRACK_POINT_SIZE - len(track_point_data):,} trackpoints\n")

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