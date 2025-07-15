
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

# Check if publisher_id is set
if len(sys.argv) > 1:
    publisher_id = sys.argv[1]
else:
    print('Please provide the publisher_id as a command line argument.')
    sys.exit(1)

# Check you are logged in to gcloud
def gcloud_login():
    try:
        subprocess.run(['gcloud', 'auth', 'login'], check=True)
    except subprocess.CalledProcessError:
        print('You are not logged in to gcloud. Please run `gcloud auth login`')
        sys.exit(1)

# Download index.txt file from gcs
def download_index():
    # If data directory does not exist, create it
    os.makedirs(f'./data/{publisher_id}', exist_ok=True)

    index_path = f'gs://earthengine-stats/providers/{publisher_id}/index.txt'
    subprocess.run(['gsutil', 'cp', index_path, f'./data/{publisher_id}/index.txt'])


# Helper function to download a single file
def download_single_file(line):
    line = line.strip()
    if not line:
        return None
    
    # Get the filename
    file = line.split('/')[-1]
    file_path = f'./data/{publisher_id}/{file}'
    
    # Check if file exists
    if os.path.exists(file_path):
        return f'{file} already exists'
    else:
        try:
            subprocess.run(['gsutil', 'cp', line, f'./data/{publisher_id}/'], check=True)
            return f'Downloaded {line}'
        except subprocess.CalledProcessError as e:
            return f'Failed to download {line}: {e}'

# Read lines of index.txt and download the files from gcs using gcloud (parallelized)
def download_files(max_workers=5):
    with open(f'./data/{publisher_id}/index.txt', 'r') as f:
        lines = [line.strip() for line in f if line.strip()]
    
    # Use ThreadPoolExecutor for parallel downloads
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all download tasks
        future_to_line = {executor.submit(download_single_file, line): line for line in lines}
        
        # Process completed downloads
        for future in as_completed(future_to_line):
            result = future.result()
            if result:
                print(result)

combined_filepath = f'./data/{publisher_id}-combined.csv'

# Combine all files into a single file
def combine_files(combined_filepath):
    with open(combined_filepath, 'w') as outfile:
        file_count = 0
        for file in os.listdir(f'./data/{publisher_id}'):
            if file.startswith('earthengine_stats'):
                with open(f'./data/{publisher_id}/{file}', 'r') as infile:
                    file_count += 1
                    for line in infile:
                        # Skip the header of the file after the first file
                        if file_count > 1 and line.startswith('Interval'):
                            continue
                        outfile.write(line)
        print(f'Combined {file_count} files into {combined_filepath}')


# Sort the combined file leaving header as is
def sort_file(combined_filepath):
    with open(combined_filepath, 'r') as file:
            header = file.readline().strip()
            # Replace Dataset with Folder
            header = header.replace('Dataset', 'Folder,Project,Type,Product,Version,ImageCollection')
            header = header.replace('Interval', 'Start,End')
            lines = file.readlines()
            # Replace first / in lines with ,
            lines = [line.replace('/', ',') for line in lines]
            sorted_lines = sorted(lines)

    with open(combined_filepath, 'w') as file:
        file.write(header + '\n')
        file.writelines(sorted_lines)





if __name__ == '__main__':
    gcloud_login()
    download_index()
    download_files()
    combine_files(combined_filepath)
    sort_file(combined_filepath)
