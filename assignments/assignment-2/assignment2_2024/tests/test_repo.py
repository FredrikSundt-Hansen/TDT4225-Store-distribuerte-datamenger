import unittest
from geolife_repo import GeolifeRop as Repo
from config import *
import os

def cout_files_lines(limit):
    total_count = 0 
    total_lines = 0  

    for root, _, files in os.walk(DATASET_PATH + "/135"):
        for file in files:
            if file.endswith('.plt'):
                file_path = os.path.join(root, file)
                with open(file_path, 'r') as f:
                    lines = f.readlines()[USER_DATA_HEADER_SIZE:]  
                    line_count = len(lines)
                    if line_count <= limit:  
                        total_count += 1 
                        total_lines += line_count 
    return total_count, total_lines

class TestRepo(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass



    def test_process_data(self):
        a, b = cout_files_lines(2500)
        print(a)
        print(b)
    
if __name__ == '__main__':
    unittest.main()