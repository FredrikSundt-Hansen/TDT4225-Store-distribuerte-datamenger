import os

def count_lines_in_plt_files(directory):
    plt_files_line_count = {}
    total_line_count = 0  # Variable to store the total number of lines

    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.plt'):
                file_path = os.path.join(root, file)
                with open(file_path, 'r') as f:
                    lines = f.readlines()[6:]  # Skip the first 6 lines
                    line_count = len(lines)
                    plt_files_line_count[file] = line_count
                    total_line_count += line_count  # Add to the total count
    
    return plt_files_line_count, total_line_count

directory = 'dataset/Data/135'
line_counts, total_lines = count_lines_in_plt_files(directory)

print(f"Total lines (excluding headers) across all .plt files: {total_lines}")