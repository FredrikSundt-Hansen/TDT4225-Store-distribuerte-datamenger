from DbConnector import DbConnector
from const import USER_COLLECTION, ACTIVITY_COLLECTION, TRACKPOINT_COLLECTION
from datetime import datetime

class GeolifeDB:
    def __init__(self):
        self.connection = None
        self.client = None
        self.db = None

    def __enter__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.connection.close_connection()

    def get_collection(self, collection_name):
        """Returns the specified collection from the database."""
        return self.db[collection_name]

    def create_collections(self):
        # Create collections if they don't already exist
        if USER_COLLECTION not in self.db.list_collection_names():
            self.db.create_collection(USER_COLLECTION)
        if ACTIVITY_COLLECTION not in self.db.list_collection_names():
            self.db.create_collection(ACTIVITY_COLLECTION)
        if TRACKPOINT_COLLECTION not in self.db.list_collection_names():
            self.db.create_collection(TRACKPOINT_COLLECTION)

        self.db[ACTIVITY_COLLECTION].create_index("user_id")
        self.db[TRACKPOINT_COLLECTION].create_index("activity_id")

    def show_collections(self):
        print("Collections:", self.db.list_collection_names())

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
            activity_data (list): List of tuples containing activity details:
                (activity_id, user_id, transportation_mode, start_date_time, end_date_time)
        """
        activity_docs = []
        for activity_id, user_id, transportation_mode, start_date_time_dt, end_date_time_dt in activity_data:
            activity_doc = {
                "_id": activity_id,
                "user_id": user_id,
                "transportation_mode": transportation_mode,
                "start_date_time": start_date_time_dt,
                "end_date_time": end_date_time_dt
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
                "lat": float(tp[1]),
                "lon": float(tp[2]),
                "altitude": float(tp[3]) if tp[3] else None,
                "date_days": float(tp[4]),
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