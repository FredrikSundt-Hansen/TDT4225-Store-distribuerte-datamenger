import unittest
from geolife_repo import GeolifeRepo
from geolife_service import GeolifeService

"""
Assignment 2 taks

NB! Database needs to be populated before running tests

Below are values laready now from task description
or counting functions, see file_counter.py
"""
n_users = 182
n_activites = 16048
n_track_points = 9681756

class Assignment2Tasks(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.db = GeolifeRepo()  
        cls.db.__enter__()  

    @classmethod
    def tearDownClass(cls):
        cls.db.__exit__(None, None, None) 

    def test_task1(self):
        query = """
        SELECT COUNT(*) FROM user;
        """
        count = self.db.exec_query(query)[0]
        assert count[0] == n_users

        query = """
        SELECT COUNT(*) FROM activity;
        """
        count = self.db.exec_query(query)[0]  
        assert count[0] == n_activites

        query = """
        SELECT COUNT(*) FROM track_point;
        """
        count = self.db.exec_query(query)[0]  
        assert count[0] == n_track_points

if __name__ == '__main__':
    unittest.main()
    