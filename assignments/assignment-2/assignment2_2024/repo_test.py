import unittest
from config import *

class TestRepo(unittest.TestCase):

    def find_plt_files_with_few_lines(directory, max_lines=2500):
        plt_files_with_few_lines = []  # List to store files with lines <= max_lines
        total_count = 0  # Variable to keep track of the number of files
        total_lines = 0  # Variable to keep track of total lines for all matching files

        for root, _, files in os.walk(directory):
            for file in files:
                if file.endswith('.plt'):
                    file_path = os.path.join(root, file)
                    with open(file_path, 'r') as f:
                        lines = f.readlines()[6:]  # Skip the first 6 lines
                        line_count = len(lines)
                        if line_count <= max_lines:  # Check if lines <= max_lines
                            plt_files_with_few_lines.append((file, line_count))
                            total_count += 1  # Increment count for each matching file
                            total_lines += line_count  # Add to the total line count

        return plt_files_with_few_lines, total_count, total_lines


    files_with_few_lines, total_count, total_lines = find_plt_files_with_few_lines(DATASET_PATH)

    pass