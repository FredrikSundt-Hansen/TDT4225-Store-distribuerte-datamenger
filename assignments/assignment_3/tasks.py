import unittest
from main import GeolifeDB
from haversine import haversine, Unit
from const import N_USERS, N_ACTIVITES, N_TRACK_POINTS

"""
Assignment 3 tasks

NB! Database needs to be populated from main.py before running tests. 
"""
class Assignment2Tasks(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.db = GeolifeDB()  
        cls.db.__init__()
        cls.db.show_collections()
        #close db
        

    @classmethod
    def tearDownClass(cls):
        cls.db.__exit__(None, None, None) # Close connection
        

    """
    How many users, activities and trackpoints are there in the dataset (after it is
    inserted into the database).
    """
    def test_task1(self):
        count_users = self.db.db["User"].count_documents({})
        count_activities = self.db.db["Activity"].count_documents({})
        count_track_points = self.db.db["TrackPoint"].count_documents({})
        self.assertEqual(count_users, N_USERS)
        self.assertEqual(count_activities, N_ACTIVITES)
        self.assertEqual(count_track_points, N_TRACK_POINTS)


        

        print(f"\nTask 1, number of users: {count_users}, activities: {count_activities}, track points: {count_track_points}")

if __name__ == '__main__':
    unittest.main()
    