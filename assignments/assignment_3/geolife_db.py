from DbConnector import DbConnector
from const import USER_COLLECTION, ACTIVITY_COLLECTION, TRACKPOINT_COLLECTION


class GeolifeDB:
    def __enter__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.connection.close_connection()

    def create_collections(self):
        # Create collections if they don't already exist
        if USER_COLLECTION not in self.db.list_collection_names():
            self.db.create_collection("User")
        if ACTIVITY_COLLECTION not in self.db.list_collection_names():
            self.db.create_collection("Activity")
        if TRACKPOINT_COLLECTION not in self.db.list_collection_names():
            self.db.create_collection("Trackpoint")

    def insert_users(self, user_data):
        """
        Inserts multiple user documents into the User collection.

        Parameters:
            user_data (list): List of tuples containing user_id and has_labels.
        """
        user_docs = []
        for user_id, has_labels in user_data:
            user_doc = {
                "_id": user_id,
                "has_labels": has_labels
            }
            user_docs.append(user_doc)
        self.db[USER_COLLECTION].insert_many(user_docs)

    def insert_activities(self, activity_data):
        """
        Inserts multiple activity documents into the Activity collection.

        Parameters:
            activity_data (list): List of dictionaries containing activity details.
        """
        activity_docs = []
        for activity_id, user_id, transportation_mode, start_date_time, end_date_time in activity_data:
            activity_doc = {
                "_id": activity_id,
                "user_id": user_id,
                "transportation_mode": transportation_mode,
                "start_date_time": start_date_time,
                "end_date_time": end_date_time
            }
            activity_docs.append(activity_doc)
        self.db[ACTIVITY_COLLECTION].insert_many(activity_docs)

    def insert_trackpoints(self, trackpoint_data):
        """
        Inserts multiple trackpoint documents into the Trackpoint collection in batches.

        Parameters:
            trackpoint_data (list): List of tuples containing trackpoint details.
        """
        # Convert track_point_data tuples to dictionaries
        trackpoint_data_dicts = []
        for tp in trackpoint_data:
            tp_dict = {
                "activity_id": tp[0],
                "lat": tp[1],
                "lon": tp[2],
                "altitude": tp[3],
                "date_days": tp[4],
                "date_time": tp[5]
            }
            trackpoint_data_dicts.append(tp_dict)

        batch_size = 10000
        for i in range(0, len(trackpoint_data_dicts), batch_size):
            batch = trackpoint_data_dicts[i:i + batch_size]
            self.db[TRACKPOINT_COLLECTION].insert_many(batch)

    def insert_dataset(self, user_data, activity_data, track_point_data):
        # Insert user data
        self.insert_users(user_data)

        # Insert activity data
        self.insert_activities(activity_data)

        # Insert trackpoint data
        self.insert_trackpoints(track_point_data)

    def clear_collection(self, collection_name):
        """
        Clears all documents from a specified collection without dropping the collection itself.

        Parameters:
            collection_name (str): Name of the collection to clear.
        """
        if collection_name in self.db.list_collection_names():
            self.db[collection_name].delete_many({})

    def clear_all_collections(self):
        """
        Clears all documents from the User, Activity, and Trackpoint collections.
        """
        collections = [USER_COLLECTION, ACTIVITY_COLLECTION, TRACKPOINT_COLLECTION]
        for collection in collections:
            self.clear_collection(collection)








    """
    #########################################################
    """

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
            print(f"Processing user {user_id} ({idx + 1}/182)")
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