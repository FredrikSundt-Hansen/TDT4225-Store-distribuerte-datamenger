from pprint import pprint 
from DbConnector import DbConnector
import os
import csv 
class GeolifeDB:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def create_collections(self,):
        # Create collections if not already existing
        if "User" not in self.db.list_collection_names():
            self.db.create_collection("User")
            print("Created collection: User")
        if "Activity" not in self.db.list_collection_names():
            self.db.create_collection("Activity")
            print("Created collection: Activity")
        if "Trackpoint" not in self.db.list_collection_names():
            self.db.create_collection("Trackpoint")
            print("Created collection: Trackpoint")

    def insert_user(self, user_id):
            user_doc = {
                "_id": user_id,
                "has_labels": False  # Set to True if labeled data is available (if required in future tasks)
            }
            self.db["User"].insert_one(user_doc)

    def insert_activity(self, user_id, start_time, end_time):
        activity_doc = {
            "user_id": user_id,
            "transportation_mode": None,  # Set transportation mode here if available in future tasks
            "start_date_time": start_time,
            "end_date_time": end_time,
        }
        return self.db["Activity"].insert_one(activity_doc).inserted_id  # Return the inserted activity_id

    def insert_trackpoints(self, activity_id, trackpoints):
        trackpoint_docs = [
            {
                "activity_id": activity_id,
                "lat": float(tp[0]),
                "lon": float(tp[1]),
                "altitude": int(tp[2]) if tp[2] is not None else None,
                "date_days": float(tp[3]),
                "date_time": tp[4]
            }
            for tp in trackpoints
        ]
        if trackpoint_docs:
            self.db["TrackPoint"].insert_many(trackpoint_docs)

    def process_trajectory_files(self, user_id, trajectory_folder):
        for root, _, files in os.walk(trajectory_folder):
                    for file in files:
                        if file.endswith(".plt"):
                            file_path = os.path.join(root, file)
                            # print(f"Processing file: {file_path}")  # Debugging
                            trackpoints = []
                            with open(file_path, 'r') as f:
                                reader = csv.reader(f)
                                for i, row in enumerate(reader):
                                    if i >= 6:  # Skip first 6 lines
                                        lat, lon, _, alt, date_days, date, time = row
                                        try:
                                            # Converting latitude, longitude, altitude, date_days to correct types
                                            lat = float(lat)
                                            lon = float(lon)
                                            
                                            # Handle altitude as float, and set a default if it's not valid (-777)
                                            if alt == '-777':
                                                altitude = None  # Or any default value you prefer
                                            else:
                                                altitude = float(alt)  # Use float instead of int
                                            
                                            date_days = float(date_days)
                                            date_time = f"{date} {time}"
                                            
                                            # Append the trackpoint tuple
                                            trackpoints.append((lat, lon, altitude, date_days, date_time))
                                        except ValueError as ve:
                                            print(f"Error processing row: {row} - {ve}")
                                            continue  # Skip this trackpoint if there's an error

                        # print(f"Inserted {len(trackpoints)} trackpoints for {file}")  # Debugging
                                if len(trackpoints) > 2500:
                                    continue

                            if trackpoints:  # If there are valid trackpoints
                                start_time = trackpoints[0][-1]
                                end_time = trackpoints[-1][-1]
                                activity_id = self.insert_activity(user_id, start_time, end_time)
                                self.insert_trackpoints(activity_id, trackpoints)

    def insert_data_from_dataset(self, dataset_folder):
        for idx, user_folder in enumerate(os.listdir(dataset_folder)):
            user_id = user_folder  # Use folder name as user_id
            print(f"Processing user {user_id} ({idx+1}/182)")
            self.insert_user(user_id)

            trajectory_folder = os.path.join(dataset_folder, user_folder, "Trajectory")
            if os.path.exists(trajectory_folder):
                self.process_trajectory_files(user_id, trajectory_folder)

    def fetch_documents(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find({})
        for doc in documents:
            pprint(doc)

    def drop_collection(self, collection_name):
        if collection_name in self.db.list_collection_names():
            self.db[collection_name].drop()
            print(f"Dropped collection: {collection_name}")

    def show_collections(self):
        print("Collections:", self.db.list_collection_names())

    def __exit__(self, exc_type, exc_value, traceback):
        self.connection.close_connection()


def main():
        geolifeDB = None
        try:
            geolifeDB = GeolifeDB()
            geolifeDB.create_collections()
            geolifeDB.insert_data_from_dataset("./dataset/data")  # Adjust to your dataset path
            geolifeDB.show_collections()
            geolifeDB.fetch_documents("User")  # Fetch sample data to verify
            #delete collections after¨

            
       # except Exception as e:
            #print("ERROR: Failed to use database:", e)
        finally:
            if geolifeDB:
                geolifeDB.__exit__(None, None, None)

if __name__ == '__main__':
    main()
