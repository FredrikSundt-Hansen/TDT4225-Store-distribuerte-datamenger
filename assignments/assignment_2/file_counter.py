from const import *
import os

"""
Function for counting the number of files and lines in the dataset
"""
def count_files_lines(dir, limit):
    total_count = 0 
    total_lines = 0  

    for root, _, files in os.walk(dir):
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


total_count, total_lines = count_files_lines(DATASET_PATH, MAX_TRACK_POINTS)

print(f"Total count of files: {total_count:,}")
print(f"Total count of lines: {total_lines:,}")