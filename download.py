#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.9"
# dependencies = [
#   "google-cloud-bigquery",
# ]
# ///

import argparse
import os
import re
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


def parse_args():
    parser = argparse.ArgumentParser(description='Download GEE COG stats for a publisher')
    parser.add_argument('publisher_id', help='Publisher ID (e.g. wri)')
    parser.add_argument(
        '--bigquery',
        metavar='PROJECT.DATASET.TABLE',
        help='Push combined CSV to this BigQuery table, creating it if needed',
    )
    parser.add_argument(
        '--no-auth',
        action='store_true',
        help='Skip interactive gcloud auth (use when running in CI/cloud with a pre-configured service account)',
    )
    return parser.parse_args()


_SHELL = sys.platform == 'win32'


def gcloud_login():
    result = subprocess.run(['gcloud', 'auth', 'print-access-token'], capture_output=True, shell=_SHELL)
    if result.returncode != 0:
        try:
            subprocess.run(['gcloud', 'auth', 'login'], check=True, shell=_SHELL)
        except subprocess.CalledProcessError:
            print('Failed to log in to gcloud. Please run `gcloud auth login` manually.')
            sys.exit(1)

    adc_result = subprocess.run(
        ['gcloud', 'auth', 'application-default', 'print-access-token'],
        capture_output=True, shell=_SHELL
    )
    if adc_result.returncode != 0:
        try:
            subprocess.run(['gcloud', 'auth', 'application-default', 'login'], check=True, shell=_SHELL)
        except subprocess.CalledProcessError:
            print('Failed to set up application default credentials. Please run `gcloud auth application-default login` manually.')
            sys.exit(1)


def download_index(publisher_id):
    os.makedirs(f'./data/{publisher_id}', exist_ok=True)
    index_path = f'gs://earthengine-stats/providers/{publisher_id}/index.txt'
    local_path = f'./data/{publisher_id}/index.txt'
    for attempt in range(3):
        try:
            subprocess.run(['gcloud', 'storage', 'cp', index_path, local_path], check=True, shell=_SHELL, timeout=15)
            return
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
            os.remove(local_path) if os.path.exists(local_path) else None
            if attempt == 2:
                raise
            time.sleep(2 ** attempt)


def download_single_file(publisher_id, line):
    line = line.strip()
    if not line:
        return None

    file = line.split('/')[-1]
    file_path = f'./data/{publisher_id}/{file}'

    if os.path.exists(file_path):
        return f'{file} already exists'

    for attempt in range(3):
        try:
            subprocess.run(['gcloud', 'storage', 'cp', line, f'./data/{publisher_id}/'], check=True, shell=_SHELL, timeout=15)
            return f'Downloaded {line}'
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
            os.remove(file_path) if os.path.exists(file_path) else None
            if attempt == 2:
                return f'Failed to download {line}: {e}'
            time.sleep(2 ** attempt)  # 1s, 2s


def download_files(publisher_id, max_workers=5):
    with open(f'./data/{publisher_id}/index.txt', 'r') as f:
        lines = [line.strip() for line in f if line.strip()]

    failures = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_line = {
            executor.submit(download_single_file, publisher_id, line): line
            for line in lines
        }
        for future in as_completed(future_to_line):
            result = future.result()
            if result:
                print(result)
                if result.startswith('Failed'):
                    failures.append(result)

    return failures


def combine_files(publisher_id, combined_filepath):
    with open(combined_filepath, 'w') as outfile:
        file_count = 0
        for file in os.listdir(f'./data/{publisher_id}'):
            if file.startswith('earthengine_stats'):
                with open(f'./data/{publisher_id}/{file}', 'r') as infile:
                    file_count += 1
                    for line in infile:
                        if file_count > 1 and line.startswith('Interval'):
                            continue
                        outfile.write(line)
        print(f'Combined {file_count} files into {combined_filepath}')


def sort_file(combined_filepath):
    with open(combined_filepath, 'r') as file:
        header = file.readline().strip()
        header = header.replace('Interval', 'Start,End')
        # Sanitize column names for BigQuery/Data Studio compatibility:
        cols = header.split(',')
        cols = [re.sub(r'[^a-zA-Z0-9]+', '_', col).strip('_') for col in cols]  # replace special chars with _
        cols = [f'_{col}' if col and col[0].isdigit() else col for col in cols]  # BQ field names can't start with a digit
        header = ','.join(cols)
        lines = file.readlines()
        lines = [line.replace('/', ',', 1) for line in lines] # replace first slash with comma to split Interval into Start and End
        sorted_lines = sorted(lines)

    with open(combined_filepath, 'w') as file:
        file.write(header + '\n')
        file.writelines(sorted_lines)


def push_to_bigquery(combined_filepath, table_ref):
    from google.cloud import bigquery

    parts = table_ref.split('.')
    if len(parts) != 3:
        print(f'Invalid BigQuery table reference "{table_ref}". Expected PROJECT.DATASET.TABLE')
        sys.exit(1)

    project_id, dataset_id, table_id = parts
    client = bigquery.Client(project=project_id)

    dataset = bigquery.Dataset(f'{project_id}.{dataset_id}')
    client.create_dataset(dataset, exists_ok=True)

    table_full_ref = f'{project_id}.{dataset_id}.{table_id}'
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    with open(combined_filepath, 'rb') as f:
        load_job = client.load_table_from_file(f, table_full_ref, job_config=job_config)

    load_job.result()
    table = client.get_table(table_full_ref)
    print(f'Loaded {table.num_rows} rows into {table_full_ref}')


if __name__ == '__main__':
    args = parse_args()
    combined_filepath = f'./data/{args.publisher_id}-combined.csv'

    if not args.no_auth:
        gcloud_login()
    download_index(args.publisher_id)
    failures = download_files(args.publisher_id)
    if failures:
        print(f'{len(failures)} file(s) failed to download — aborting.')
        sys.exit(1)
    combine_files(args.publisher_id, combined_filepath)
    sort_file(combined_filepath)

    if args.bigquery:
        push_to_bigquery(combined_filepath, args.bigquery)
