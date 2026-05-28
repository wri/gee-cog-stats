#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.9"
# dependencies = [
#   "google-cloud-bigquery",
#   "google-cloud-storage",
# ]
# ///

import argparse
from contextlib import contextmanager
import os
import re
import subprocess
import sys
import time


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


def format_duration(seconds):
    if seconds < 60:
        return f'{seconds:.1f}s'

    minutes, remaining_seconds = divmod(seconds, 60)
    if minutes < 60:
        return f'{int(minutes)}m {remaining_seconds:.1f}s'

    hours, remaining_minutes = divmod(minutes, 60)
    return f'{int(hours)}h {int(remaining_minutes)}m {remaining_seconds:.1f}s'


@contextmanager
def timer(label):
    start = time.perf_counter()
    print(f'{label}...')
    try:
        yield
    except Exception:
        elapsed = time.perf_counter() - start
        print(f'{label} failed after {format_duration(elapsed)}')
        raise
    finally:
        if sys.exc_info()[0] is None:
            elapsed = time.perf_counter() - start
            print(f'{label} completed in {format_duration(elapsed)}')


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


def parse_gs_uri(uri):
    if not uri.startswith('gs://'):
        raise ValueError(f'Invalid GCS URI "{uri}". Expected gs://BUCKET/OBJECT')

    bucket_name, _, blob_name = uri[5:].partition('/')
    if not bucket_name or not blob_name:
        raise ValueError(f'Invalid GCS URI "{uri}". Expected gs://BUCKET/OBJECT')

    return bucket_name, blob_name


def source_blob(storage_client, uri):
    bucket_name, blob_name = parse_gs_uri(uri)
    return storage_client.bucket(bucket_name).blob(blob_name)


def retry(operation, description):
    for attempt in range(3):
        try:
            return operation()
        except Exception as e:
            if attempt == 2:
                raise RuntimeError(f'Failed to {description}: {e}') from e
            time.sleep(2 ** attempt)


def download_index(publisher_id, storage_client):
    os.makedirs(f'./data/{publisher_id}', exist_ok=True)
    index_path = f'gs://earthengine-stats/providers/{publisher_id}/index.txt'
    local_path = f'./data/{publisher_id}/index.txt'

    index_text = retry(
        lambda: source_blob(storage_client, index_path).download_as_text(),
        f'download {index_path}',
    )
    with open(local_path, 'w') as f:
        f.write(index_text)

    lines = [line.strip() for line in index_text.splitlines() if line.strip()]
    print(f'Loaded index with {len(lines)} file(s)')
    return lines


def append_source_file(storage_client, uri, outfile, include_header):
    blob = source_blob(storage_client, uri)
    start_pos = outfile.tell()

    def append():
        outfile.seek(start_pos)
        outfile.truncate()
        with blob.open('rt') as infile:
            for line_number, line in enumerate(infile):
                if not include_header and line_number == 0 and line.startswith('Interval'):
                    continue
                outfile.write(line)

    retry(append, f'stream {uri}')


def combine_files(source_uris, storage_client, combined_filepath):
    tmp_filepath = f'{combined_filepath}.tmp'
    try:
        with open(tmp_filepath, 'w') as outfile:
            file_count = 0
            for uri in source_uris:
                filename = uri.rstrip('/').split('/')[-1]
                if not filename.startswith('earthengine_stats'):
                    continue

                append_source_file(storage_client, uri, outfile, include_header=(file_count == 0))
                file_count += 1
                print(f'Added {uri}')

            if file_count == 0:
                raise RuntimeError('No earthengine_stats files found in index')

        os.replace(tmp_filepath, combined_filepath)
        print(f'Combined {file_count} files into {combined_filepath}')
    except Exception:
        os.remove(tmp_filepath) if os.path.exists(tmp_filepath) else None
        raise


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

    with timer('Total run'):
        if not args.no_auth:
            with timer('Checking Google Cloud auth'):
                gcloud_login()

        from google.cloud import storage as gcs

        with timer('Creating GCS client'):
            gcs_client = gcs.Client()

        with timer('Loading source index'):
            source_uris = download_index(args.publisher_id, gcs_client)

        with timer('Combining source files'):
            combine_files(source_uris, gcs_client, combined_filepath)

        with timer('Sorting combined CSV'):
            sort_file(combined_filepath)

        if args.bigquery:
            with timer('Loading BigQuery table'):
                push_to_bigquery(combined_filepath, args.bigquery)
