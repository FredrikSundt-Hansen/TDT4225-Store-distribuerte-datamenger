import unittest
from main import GeolifeDB
from haversine import haversine, Unit
from const import N_USERS, N_ACTIVITIES, N_TRACK_POINTS, USER_COLLECTION, ACTIVITY_COLLECTION, TRACKPOINT_COLLECTION
from datetime import datetime
import pprint

"""
Assignment 3 tasks

NB! Database needs to be populated from main.py before running tests. 
"""
class Assignment3Tasks(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.db = GeolifeDB()
        cls.db.__enter__()

    @classmethod
    def tearDownClass(cls):
        cls.db.__exit__(None, None, None)

    def test_task1(self):
        """
        How many users, activities and trackpoints are there in the dataset (after it is
        inserted into the database).
        """
        count_users = self.db.get_collection(USER_COLLECTION).count_documents({})
        count_activities = self.db.get_collection(ACTIVITY_COLLECTION).count_documents({})
        count_track_points = self.db.get_collection(TRACKPOINT_COLLECTION).count_documents({})

        self.assertEqual(N_USERS, count_users)
        self.assertEqual(N_ACTIVITIES, count_activities)
        self.assertEqual(N_TRACK_POINTS, count_track_points)

        print(f"Task 1: count of users: {count_users:,} | count of activities: {count_activities:,} | "
              f"count of track points: {count_track_points:,}")

    def test_task2(self):
        """
        Find the average number of activities per user.
        """
        activity_count = self.db.get_collection(ACTIVITY_COLLECTION).count_documents({})
        user_count = self.db.get_collection(USER_COLLECTION).count_documents({})
        average = activity_count / user_count if user_count else 0

        print(f"Task 2: Average number of activities per user: {average:.2f}")
        pprint.pp(average)

    def test_task3(self):
        """
        Find the top 20 users with the highest number of activities.
        """
        pipeline = [
            {"$group": {"_id": "$user_id", "activity_count": {"$sum": 1}}},
            {"$sort": {"activity_count": -1}},
            {"$limit": 20}
        ]
        results = list(self.db.get_collection(ACTIVITY_COLLECTION).aggregate(pipeline))

        print("Task 3: Top 20 users with the highest number of activities:")
        pprint.pp(results)

    def test_task4(self):
        """
        Find all users who have taken a taxi.
        """
        users = self.db.get_collection(ACTIVITY_COLLECTION).distinct("user_id", {"transportation_mode": "taxi"})

        print("Task 4: All users who have taken a taxi:")
        pprint.pp(users)

    def test_task5(self):
        """
        Find all types of transportation modes and count how many activities that are
        tagged with these transportation mode labels. Do not count the rows where
        the mode is null.
        """
        pipeline = [
            {"$match": {"transportation_mode": {"$ne": None}}},
            {"$group": {"_id": "$transportation_mode", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}  # Sort by count in descending order
        ]
        results = list(self.db.get_collection(ACTIVITY_COLLECTION).aggregate(pipeline))

        print("Task 5: All types of transportation modes and their activity counts:")
        pprint.pp(results)

    def test_task6a(self):
        """
        Find the year with the most activities.
        """
        pipeline = [
            {"$project": {"year": {"$year": "$start_date_time"}}},
            {"$group": {"_id": "$year", "activity_count": {"$sum": 1}}},
            {"$sort": {"activity_count": -1}},
            {"$limit": 1}
        ]
        result = self.db.get_collection(ACTIVITY_COLLECTION).aggregate(pipeline).next()

        print("Task 6a: Year with the most activities:")
        pprint.pp(result)

    def test_task6b(self):
        """
        Is this also the year with most recorded hours?
        """
        pipeline = [
            {"$project": {
                "year": {"$year": "$start_date_time"},
                "duration_hours": {
                    "$divide": [
                        {"$subtract": ["$end_date_time", "$start_date_time"]},
                        1000 * 60 * 60  # Convert milliseconds to hours
                    ]
                }
            }},
            {"$group": {"_id": "$year", "total_hours": {"$sum": "$duration_hours"}}},
            {"$sort": {"total_hours": -1}},
            {"$limit": 1}
        ]
        result = self.db.get_collection(ACTIVITY_COLLECTION).aggregate(pipeline).next()

        print("Task 6b: Year with most recorded hours:")
        pprint.pp(result)

    def test_task7(self):
        """
        Find the total distance (in km) walked in 2008, by user with id=112.
        """
        activities = self.db.get_collection(ACTIVITY_COLLECTION).find({
            "user_id": "112",
            "transportation_mode": "walk",
            "start_date_time": {"$gte": datetime(2008, 1, 1), "$lt": datetime(2009, 1, 1)}
        }, {"_id": 1})

        total_distance = 0.0
        for activity in activities:
            trackpoints_cursor = self.db.get_collection(TRACKPOINT_COLLECTION).find(
                {"activity_id": activity["_id"]},
                {"lat": 1, "lon": 1, "date_time": 1}
            ).sort("date_time", 1)

            prev_point = None
            for tp in trackpoints_cursor:
                current_point = (tp["lat"], tp["lon"])
                if prev_point is not None:
                    distance = haversine(prev_point, current_point)
                    total_distance += distance
                prev_point = current_point

        print("Task 7: Total distance walked by user 112 in 2008:")
        pprint.pp(total_distance)

    def test_task8(self):
        """
        Find the top 20 users who have gained the most altitude meters.
        """
        users = self.db.get_collection(USER_COLLECTION).find({}, {"_id": 1})
        user_gains = []

        for user in users:
            user_id = user["_id"]
            activities = self.db.get_collection(ACTIVITY_COLLECTION).find({"user_id": user_id}, {"_id": 1})
            total_gain = 0.0

            for activity in activities:
                trackpoints_cursor = self.db.get_collection(TRACKPOINT_COLLECTION).find(
                    {"activity_id": activity["_id"], "altitude": {"$ne": None}},
                    {"altitude": 1, "date_time": 1}
                ).sort("date_time", 1)

                prev_altitude = None
                for tp in trackpoints_cursor:
                    curr_altitude = tp["altitude"]
                    if prev_altitude is not None and curr_altitude > prev_altitude:
                        total_gain += curr_altitude - prev_altitude
                    prev_altitude = curr_altitude

            user_gains.append({"user_id": user_id, "total_gain": total_gain})

        # Sort the users by total_gain
        top_users = sorted(user_gains, key=lambda x: x["total_gain"], reverse=True)[:20]

        print("Task 8: Top 20 users who have gained the most altitude meters:")
        pprint.pp(top_users)

    def test_task9(self):
        """
        Find all users who have invalid activities, and the number of invalid activities per user.
        - An invalid activity is defined as an activity with consecutive trackpoints where the timestamps deviate with
        at least 5 minutes.
        """
        activities = self.db.get_collection(ACTIVITY_COLLECTION).find({}, {"_id": 1, "user_id": 1})
        user_invalid_counts = {}

        for activity in activities:
            activity_id = activity["_id"]
            user_id = activity["user_id"]

            trackpoints_cursor = self.db.get_collection(TRACKPOINT_COLLECTION).find(
                {"activity_id": activity_id},
                {"date_time": 1}
            ).sort("date_time", 1)

            prev_date_time = None
            invalid = False
            for tp in trackpoints_cursor:
                current_date_time = tp["date_time"]
                if prev_date_time is not None:
                    time_diff = (current_date_time - prev_date_time).total_seconds()
                    if time_diff >= 300:  # 5 minutes in seconds
                        invalid = True
                        break
                prev_date_time = current_date_time

            if invalid:
                user_invalid_counts[user_id] = user_invalid_counts.get(user_id, 0) + 1

        print("Task 9: Users with invalid activities:")
        pprint.pp(user_invalid_counts)

    def test_task10(self):
        """
        Find the users who have tracked an activity in the Forbidden City of Beijing.
        - In this question, consider the Forbidden City to have
        coordinates that correspond to: lat 39.916, lon 116.397.

        Instead of checking if every user has been exactly at that point, consider the area of the city.

        The Forbidden City of Beijing covers 72 hectares or 0.72 square kilometers.
        Reference: https://en.wikipedia.org/wiki/Forbidden_City

        The square root of 0.72 is approximately 0.849 km.
        Thus, consider a radius of 0.424 km in each direction for simplicity.
        """
        from geopy.distance import distance
        from geopy.point import Point

        forbidden_city_coords = (39.916, 116.397)
        radius_km = 0.424  # Radius in kilometers

        # Define the center point
        center_point = Point(forbidden_city_coords[0], forbidden_city_coords[1])

        # Calculate the bounding coordinates
        north = distance(kilometers=radius_km).destination(center_point, 0).latitude
        south = distance(kilometers=radius_km).destination(center_point, 180).latitude
        east = distance(kilometers=radius_km).destination(center_point, 90).longitude
        west = distance(kilometers=radius_km).destination(center_point, 270).longitude

        lat_min = min(south, north)
        lat_max = max(south, north)
        lon_min = min(west, east)
        lon_max = max(west, east)

        # Query trackpoints within the latitude and longitude range
        trackpoints_cursor = self.db.get_collection(TRACKPOINT_COLLECTION).find({
            'lat': {'$gte': lat_min, '$lte': lat_max},
            'lon': {'$gte': lon_min, '$lte': lon_max}
        }, {'activity_id': 1, 'lat': 1, 'lon': 1})

        activity_ids = set()
        for tp in trackpoints_cursor:
            lat = tp['lat']
            lon = tp['lon']
            # Calculate distance to the Forbidden City center
            current_point = (lat, lon)
            dist = haversine(forbidden_city_coords, current_point, unit=Unit.KILOMETERS)
            if dist <= radius_km:
                activity_ids.add(tp['activity_id'])

        # Get the user_ids from the activities
        activities_cursor = self.db.get_collection(ACTIVITY_COLLECTION).find(
            {'_id': {'$in': list(activity_ids)}},
            {'user_id': 1}
        )
        valid_users = set(activity['user_id'] for activity in activities_cursor)

        print(f"\nTask 10: number of users who have been in The Forbidden City of Beijing: {len(valid_users)}")
        pprint.pp(valid_users)


    def test_task11(self):
        """
        Find all users who have registered transportation_mode and their most used transportation_mode.
        - The answer should be on format (user_id, most_used_transportation_mode) sorted on user_id.
        - Some users may have the same number of activities tagged with e.g. walk and car. In this case it is up to you
        to decide which transportation mode to include in your answer (choose one).
        - Do not count the rows where the mode is null.
        """
        pipeline = [
            {"$match": {"transportation_mode": {"$ne": None}}},
            {"$group": {
                "_id": {
                    "user_id": "$user_id",
                    "transportation_mode": "$transportation_mode"
                },
                "count": {"$sum": 1}
            }},
            {"$sort": {"_id.user_id": 1, "count": -1}},
            {"$group": {
                "_id": "$_id.user_id",
                "transportation_mode": {"$first": "$_id.transportation_mode"},
                "count": {"$first": "$count"}
            }},
            {"$sort": {"_id": 1}}
        ]
        result = list(self.db.get_collection(ACTIVITY_COLLECTION).aggregate(pipeline))

        print("Task 11: Users and their most used transportation mode:")
        pprint.pp(result)

if __name__ == '__main__':
    unittest.main()
    