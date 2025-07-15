
import os
import subprocess
import sys

publisher_id = ''

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


# Read lines of index.txt and download the files from gcs using gcloud
def download_files():
    with open(f'./data/{publisher_id}/index.txt', 'r') as f:
        for line in f:
            line = line.strip()
            # Get the filename
            file = line.split('/')[-1]
            # Check if file exists
            if os.path.exists(file):
                print(f'{file} already exists')
            elif line:
                print(f'Downloading {line}')
                subprocess.run(['gsutil', 'cp', line, f'./data/{publisher_id}/'])

combined_filepath = f'./data/{publisher_id}-combined.csv'

# Combine all files into a single file
def combine_files(combined_filepath):
    with open(combined_filepath, 'w') as outfile:
        file_count = 0
        for file in os.listdir(f'./data/{publisher_id}'):
            if file.startswith('earthengine_stats'):
                with open(file, 'r') as infile:
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
