import unittest
from main import GeolifeDB
from haversine import haversine, Unit
from const import N_USERS, N_ACTIVITIES, N_TRACK_POINTS, USER_COLLECTION, ACTIVITY_COLLECTION, TRACKPOINT_COLLECTION
from datetime import datetime

"""
Assignment 3 tasks

NB! Database needs to be populated from main.py before running tests. 
"""
class Assignment3Tasks(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.db = GeolifeDB()
        cls.db.__enter__()
        cls.db.show_collections()

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

        print(f"\nTask 1, number of users: {count_users}, activities: {count_activities}, track points: {count_track_points}")

    def test_task2(self):
        """
        Find the average number of activities per user.
        """
        activity_count = self.db.get_collection(ACTIVITY_COLLECTION).count_documents({})
        user_count = self.db.get_collection(USER_COLLECTION).count_documents({})
        average = activity_count / user_count if user_count else 0
        print(f"Average number of activities per user: {average:.2f}")

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
        for result in results:
            print(f"User ID: {result['_id']}, Activity Count: {result['activity_count']}")

    def test_task4(self):
        """
        Find all users who have taken a taxi.
        """
        users = self.db.get_collection(ACTIVITY_COLLECTION).distinct("user_id", {"transportation_mode": "taxi"})
        print("Users who have taken a taxi:", users)

    def test_task5(self):
        """
        Find all types of transportation modes and count how many activities that are
        tagged with these transportation mode labels. Do not count the rows where
        the mode is null.
        """
        pipeline = [
            {"$match": {"transportation_mode": {"$ne": None}}},
            {"$group": {"_id": "$transportation_mode", "count": {"$sum": 1}}}
        ]
        results = list(self.db.get_collection(ACTIVITY_COLLECTION).aggregate(pipeline))
        for result in results:
            print(f"Mode: {result['_id']}, Activity Count: {result['count']}")

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
        print(f"Year with most activities: {result['_id']} ({result['activity_count']} activities)")

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
        print(f"Year with most recorded hours: {result['_id']} ({result['total_hours']:.2f} hours)")

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
            trackpoints = list(self.db.get_collection(TRACKPOINT_COLLECTION).find(
                {"activity_id": activity["_id"]},
                {"lat": 1, "lon": 1}
            ).sort("date_time", 1))
            for i in range(1, len(trackpoints)):
                coord1 = (trackpoints[i - 1]["lat"], trackpoints[i - 1]["lon"])
                coord2 = (trackpoints[i]["lat"], trackpoints[i]["lon"])
                distance = haversine(coord1, coord2)
                total_distance += distance

        print(f"Total distance walked by user 112 in 2008: {total_distance:.2f} km")

    def test_task8(self):
        """
        Find the top 20 users who have gained the most altitude meters
        """
        users = self.db.get_collection(USER_COLLECTION).find({}, {"_id": 1})
        user_gains = []
        for user in users:
            user_id = user["_id"]
            activities = self.db.get_collection(ACTIVITY_COLLECTION).find({"user_id": user_id}, {"_id": 1})
            total_gain = 0.0
            for activity in activities:
                trackpoints = list(self.db.get_collection(TRACKPOINT_COLLECTION).find(
                    {"activity_id": activity["_id"], "altitude": {"$ne": None}},
                    {"altitude": 1}
                ).sort("date_time", 1))
                for i in range(1, len(trackpoints)):
                    prev_altitude = trackpoints[i - 1]["altitude"]
                    curr_altitude = trackpoints[i]["altitude"]
                    if curr_altitude > prev_altitude:
                        total_gain += curr_altitude - prev_altitude
            user_gains.append({"user_id": user_id, "total_gain": total_gain})
        # Sort the users by total_gain
        top_users = sorted(user_gains, key=lambda x: x["total_gain"], reverse=True)[:20]
        for user in top_users:
            print(f"User ID: {user['user_id']}, Total Altitude Gain: {user['total_gain']:.2f} meters")

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
            trackpoints = list(self.db.get_collection(TRACKPOINT_COLLECTION).find(
                {"activity_id": activity_id},
                {"date_time": 1}
            ).sort("date_time", 1))
            invalid = False
            for i in range(1, len(trackpoints)):
                time_diff = (trackpoints[i]["date_time"] - trackpoints[i - 1]["date_time"]).total_seconds()
                if time_diff >= 300:  # 5 minutes in seconds
                    invalid = True
                    break
            if invalid:
                user_invalid_counts[user_id] = user_invalid_counts.get(user_id, 0) + 1
        for user_id, count in user_invalid_counts.items():
            print(f"User ID: {user_id}, Invalid Activities: {count}")

    def test_task10(self):
        """
        Find the users who have tracked an activity in the Forbidden City of Beijing.
        In this question, consider the Forbidden City to have
        coordinates that correspond to: lat 39.916, lon 116.397.

        Instead of checking if every user has been exactly at that point, consider the area of the city.

        The Forbidden City of Beijing covers 72 hectares or 0.72 square kilometers.
        Reference: https://en.wikipedia.org/wiki/Forbidden_City

        The square root of 0.72 is approximately 0.849 km.
        Thus, consider a radius of 0.424 km in each direction for simplicity.
        """

        forbidden_city = (39.916, 116.397)
        valid_users = set()

        pipeline = [
            {
                "$lookup": {
                    "from": "Activity",
                    "localField": "activity_id",
                    "foreignField": "_id",
                    "as": "activity"
                }
            },
            {
                "$unwind": "$activity"
            },
            {
                "$project": {
                    "user_id": "$activity.user_id",
                    "lat": {"$round": ["$lat", 3]},
                    "lon": {"$round": ["$lon", 3]}
                }
            },
            {
                "$match": {
                    "lat": 39.916,
                    "lon": 116.397
                }
            }
        ]

        results = list(self.db.get_collection(TRACKPOINT_COLLECTION).aggregate(pipeline))

        for result in results:
            user_id = result["user_id"]
            current_point = (result["lat"], result["lon"])
            dist = haversine(forbidden_city, current_point, Unit.KILOMETERS)
            if dist <= 0.424:
                valid_users.add(user_id)

        assert len(valid_users) > 0

        print(f"\nTask 10, number of users who have been in The Forbidden City of Beijing: {len(valid_users)}\n"
              f"Users: {valid_users}")


    def test_task11(self):
        """
        Find all users who have registered transportation_mode and their most used transportation_mode.
        - The answer should be on format (user_id, most_used_transportation_mode) sorted on user_id.
        - Some users may have the same number of activities tagged with e.g. walk and car.
        In this case it is up to you to decide which transportation mode to include in your answer (choose one).
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
        results = list(self.db.get_collection(ACTIVITY_COLLECTION).aggregate(pipeline))
        for result in results:
            print(f"User ID: {result['_id']}, Most Used Mode: {result['transportation_mode']} ({result['count']} activities)")

if __name__ == '__main__':
    unittest.main()
    