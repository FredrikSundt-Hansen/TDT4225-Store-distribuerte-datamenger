import os
import hashlib

directory = 'dataset/Data/'

def hash_file(file_path):
    """Generate hash for a file."""
    hasher = hashlib.md5()  # You can use hashlib.sha256() for a stronger hash
    with open(file_path, 'rb') as f:
        while chunk := f.read(8192):
            hasher.update(chunk)
    return hasher.hexdigest()

def find_duplicate_files(directory):
    file_hashes = {}  # Dictionary to store hash -> list of file paths
    duplicates = []   # List to store duplicate file paths

    for root, _, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            file_hash = hash_file(file_path)
            
            if file_hash in file_hashes:
                # If the hash already exists, it means the file is a duplicate
                duplicates.append(file_path)
                file_hashes[file_hash].append(file_path)
            else:
                file_hashes[file_hash] = [file_path]

    return duplicates

"""
duplicates = find_duplicate_files(directory)

if duplicates:
    print("Duplicate files found:")
    for duplicate in duplicates:
        print(duplicate)
else:
    print("No duplicate files found.")

"""


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


files_with_few_lines, total_count, total_lines = find_plt_files_with_few_lines(directory)

# Print out the total count and total line count
print(f"Total number of files with lines <= 2500: {total_count}")
print(f"Total number of lines (excluding headers) across matching files: {total_lines}")