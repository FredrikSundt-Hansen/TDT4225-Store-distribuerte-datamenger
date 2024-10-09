import unittest
from unittest.mock import patch, MagicMock, mock_open
from repo import Repo 
from config import *
import os

def cout_files_lines(limit):
    total_count = 0 
    total_lines = 0  

    for root, _, files in os.walk(DATASET_PATH):
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
        self.repo = Repo()
        self.repo.setup_schema()

    def tearDown(self):
        self.repo.delete_from_user_table()
        self.repo.connection.close_connection()



    def test_process_data(self):
        a, b = cout_files_lines(2500)
        print(a)
        print(b)
        #self.repo.process_dataset(limit=200)



    
if __name__ == '__main__':
    unittest.main()