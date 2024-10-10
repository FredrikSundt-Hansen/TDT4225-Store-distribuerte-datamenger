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
    Find the average number of activities per user.
    """
    def test_task2(self):
        query ="""
        SELECT AVG(activity_count) AS avg_activities_per_user
        FROM (
            SELECT COUNT(*) AS activity_count
            FROM activity
            GROUP BY user_id
        ) AS user_activity_count;
        """
        res = self.db.exec_query(query)
        assert len(res) > 0

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
        SELECT transportation_mode, COUNT(*) AS count_transportation_mode
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
        ORDER BY date_time;
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
    Find the top 20 users who have gained the most altitude meters. 
    Output should be a table with (id, total meters gained per user). Remember that some altitude-values are invalid.
    """
    def test_task8(self):
        pass
    
    """
    Find all users who have invalid activities, and the number of invalid activities per user 
    An invalid activity is defined as an activity with consecutive track points where the 
    timestamps deviate with at least 5 minutes. 
    """
    def test_task9(self):
        query = """
        WITH consecutive_track_point AS (
            SELECT tp_current.activity_id
            FROM track_point tp_current
            JOIN track_point tp_next ON tp_next.activity_id = tp_current.activity_id
            AND tp_next.id = tp_current.id + 1
            WHERE TIMESTAMPDIFF(Minute, tp_current.date_time, tp_next.date_time) >= 5
        )
        
        SELECT a.user_id, COUNT(DISTINCT a.id) AS count_invalid_activities
        FROM activity a
        JOIN consecutive_track_point cpt ON a.id = cpt.activity_id
        GROUP BY a.user_id;
        """
        res = self.db.exec_query(query)
        assert len(res) > 0 and len(res[0]) == 2

    """
    Find the users who have tracked an activity in the Forbidden City of Beijing. 
    In this question you can consider the Forbidden City to have coordinates that 
    correspond to: lat 39.916, lon 116.397.

    Instead of checking if every user has been exactly at that point, consider the area of the city.
    
    The Forbidden City of Beijing covers 72 hectares or 0.72 square kilometers.
    Reference: https://en.wikipedia.org/wiki/Forbidden_City
    
    The square root of 0.72 is approximately 0.849.
    Thus, consider a radius of 0.424 km in each direction for simplicity.
    """
    def test_task10(self):
        query = """
        SELECT user_id, lat, lon
        FROM track_point tp
        JOIN activity a ON a.id = tp.activity_id
        WHERE ROUND(lat, 3) = 39.916
        AND ROUND(lon, 3) = 116.397;
        """
        res = self.db.exec_query(query)
        assert len(res) > 0

        forbidden_city = (39.916, 116.397)
        valid_users = set()

        for user_id, lat, lon in res:
            current_point = (lat, lon)
            dist = haversine(forbidden_city, current_point, Unit.KILOMETERS)
            if dist <= 0.424:
                valid_users.add(user_id)

        print(f"\nTask 10, number of users who have been in The Forbidden City of Beijing: {len(valid_users)}\n"
              f"Users: {valid_users}")


    """
    Find all users who have registered transportation_mode and their most used 
    transportation_mode. The answer should be on format 
    (user_id, most_used_transportation_mode) sorted on user_id. Some users 
    may have the same number of activities tagged with e.g. walk and car. 
    In this case it is up to you to decide which transportation mode to include in 
    your answer (choose one). Do not count the rows where the mode is null 
    """
    def test_task11(self):
        query = """
        WITH user_transportation_modes AS (
            SELECT
                user_id, transportation_mode, COUNT(transportation_mode) AS transport_mode_count
            FROM activity
            WHERE transportation_mode IS NOT NULL
            GROUP BY user_id, transportation_mode
        ),
        most_used_modes AS (
            SELECT user_id, transportation_mode, transport_mode_count,
                ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY transport_mode_count DESC) AS rn
            FROM user_transportation_modes
        )
        
        SELECT user_id, transportation_mode AS most_used_transportation_mode
        FROM most_used_modes
        WHERE rn = 1
        ORDER BY user_id;
        """
        res = self.db.exec_query(query)
        assert len(res) > 0 and len(res[0]) == 2


if __name__ == '__main__':
    unittest.main()
    