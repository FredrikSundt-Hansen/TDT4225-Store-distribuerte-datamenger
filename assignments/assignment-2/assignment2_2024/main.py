from geolife_repo import GeolifeRop as Repo
from geolife_service import GeolifeService as Svc
from config import ORIGINAL_TRACK_POINT_SIZE

def main():
    svc = Svc()
    user_data, activity_data, track_point_data = svc.process_dataset(limit=200)

    print(f"Count of users: {len(user_data):,}")
    print(f"Count of activities: {len(activity_data):,}")
    print(f"Count of track points: {len(track_point_data):,}")
    print(f"Cleaned: {ORIGINAL_TRACK_POINT_SIZE - len(track_point_data):,} trackpoints\n")

    try:
        with Repo() as repo:
            repo.setup_schema()
            print("Schema setup complete")
            repo.insert_dataset(user_data, activity_data, track_point_data)
            print("Dataset inserted")
            #repo.clean_db()
            #print("Cleaned db")
    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == '__main__':
    main()