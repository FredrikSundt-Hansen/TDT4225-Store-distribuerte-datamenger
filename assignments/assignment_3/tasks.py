import unittest
from main import GeolifeDB
from haversine import haversine, Unit
from const import N_USERS, N_ACTIVITIES, N_TRACK_POINTS

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


    """
    How many users, activities and trackpoints are there in the dataset (after it is
    inserted into the database).
    """
    def test_task1(self):
        count_users = self.db.db["User"].count_documents({})
        count_activities = self.db.db["Activity"].count_documents({})
        count_track_points = self.db.db["Trackpoint"].count_documents({})

        self.assertEqual(N_USERS, count_users)
        self.assertEqual(N_ACTIVITIES, count_activities)
        self.assertEqual(N_TRACK_POINTS, count_track_points)

        print(f"\nTask 1, number of users: {count_users}, activities: {count_activities}, track points: {count_track_points}")


if __name__ == '__main__':
    unittest.main()
    