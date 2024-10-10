import unittest
from geolife_db import GeolifeDB
from haversine import haversine, Unit
from const import N_USERS, N_ACTIVITES, N_TRACK_POINTS

"""
Assignment 2 tasks

NB! Database needs to be populated before running tests
"""

class Assignment2Tasks(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.db = GeolifeDB()  
        cls.db.__enter__()  
        cls.db.show_tables()

    @classmethod
    def tearDownClass(cls):
        cls.db.__exit__(None, None, None) 

    """
    How many users, activities and trackpoints are there in the dataset (after it is
    inserted into the database).
    """
    def test_task1(self):
        query = """
        SELECT COUNT(*) FROM user;
        """
        count = self.db.exec_query(query)[0]
        assert count[0] == N_USERS

        query = """
        SELECT COUNT(*) FROM activity;
        """
        count = self.db.exec_query(query)[0]  
        assert count[0] == N_ACTIVITES

        query = """
        SELECT COUNT(*) FROM track_point;
        """
        count = self.db.exec_query(query)[0]  
        assert count[0] == N_TRACK_POINTS
    
    """
    Find the top 20 users with the highest number of activities. 
    """
    def test_task3(self):
        query = """
        SELECT u.id, COUNT(*) AS count_activity 
        FROM user u 
        JOIN activity a ON u.id = a.user_id 
        GROUP BY u.id 
        ORDER BY count_activity DESC 
        LIMIT 20;
        """
        res = self.db.exec_query(query)
        assert len(res) == 20

    """
    Find all types of transportation modes and count how many activities that are 
    tagged with these transportation mode labels. Do not count the rows where 
    the mode is null.
    """
    def test_task5(self):
        query = """
        SELECT transportation_mode, COUNT(*) AS count
        FROM activity 
        WHERE transportation_mode IS NOT NULL 
        GROUP BY transportation_mode;
        """
        res = self.db.exec_query(query)
        assert len(res) > 0


    """
    Find the total distance (in km) walked in 2008, by user with id=112
    """
    def test_task7(self):
        query = """
        SELECT lat, lon 
        FROM track_point 
        JOIN activity ON track_point.activity_id = activity.id 
        WHERE user_id = '112' 
        AND transportation_mode = 'walk' 
        AND YEAR(start_date_time) = 2008 
        AND YEAR(end_date_time) = 2008 
        ORDER BY date_time ASC;
        """
        res = self.db.exec_query(query)
        assert len(res) != 0

        previous_point = None
        total_dist = 0
        for lat, lon in res:
            current_point = (lat, lon)
            if previous_point:
                total_dist += haversine(previous_point, current_point, unit=Unit.KILOMETERS)
            previous_point = current_point

        assert total_dist is not None
        assert total_dist > 0

        print(f"\nTask 7, total distance walked by user 112 in 2008: {total_dist:.3f} km")
    
    """
    Find all users who have invalid activities, and the number of invalid activities per user 
    An invalid activity is defined as an activity with consecutive trackpoints where the 
    timestamps deviate with at least 5 minutes. 
    """
    def test_task9(self):
        query = """
        WITH consecutive_track_point AS (
            SELECT tp_current.activity_id
            FROM track_point tp_current
            JOIN track_point tp_next ON tp_current.activity_id = tp_next.activity_id
            AND tp_next.id = tp_current.id + 1
            WHERE TIMESTAMPDIFF(Minute, tp_current.date_time, tp_next.date_time) >= 5
        )

        SELECT a.user_id, COUNT(*) AS count
        FROM activity a
        JOIN consecutive_track_point cpt ON a.id = cpt.activity_id 
        GROUP BY a.user_id;
        """
        res = self.db.exec_query(query)
        assert res is not None

        print(f"\nTask 9, count of users with invalid activities {len(res)}")

    """
    Find all users who have registered transportation\_mode and their most used 
    transportation\_mode. The answer should be on format 
    (user\_id, most\_used\_transportation\_mode) sorted on user\_id. Some users 
    may have the same number of activities tagged with e.g. walk and car. 
    In this case it is up to you to decide which transportation mode to include in 
    your answer (choose one). Do not count the rows where the mode is null 
    """
    def test_task11(self):
        query = """
        WITH user_transportation_modes AS (
            SELECT 
                user_id, transportation_mode, COUNT(transportation_mode) AS count
            FROM activity
            WHERE transportation_mode IS NOT NULL
            GROUP BY user_id, transportation_mode
        ),
        most_used_modes AS (
            SELECT user_id, transportation_mode, count,
                ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY count DESC) AS rn
            FROM user_transportation_modes
        )

        SELECT user_id, transportation_mode
        FROM most_used_modes
        WHERE rn = 1
        ORDER BY user_id ASC;
        """
        res = self.db.exec_query(query)
        assert len(res) > 0


if __name__ == '__main__':
    unittest.main()
    