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
from datetime import datetime, timezone
import os
import re
import subprocess
import sys
import time
import tempfile
import uuid


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
    parser.add_argument(
        '--incremental',
        action='store_true',
        help='Only ingest new source files into BigQuery, using BigQuery metadata tables for state',
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


def index_uri_for_publisher(publisher_id):
    return f'gs://earthengine-stats/providers/{publisher_id}/index.txt'


def is_stats_uri(uri):
    filename = uri.rstrip('/').split('/')[-1]
    return filename.startswith('earthengine_stats')


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
    index_path = index_uri_for_publisher(publisher_id)
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
                if not is_stats_uri(uri):
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


def sanitize_header(header):
    header = header.strip()
    header = header.replace('Interval', 'Start,End')
    cols = header.split(',')
    cols = [re.sub(r'[^a-zA-Z0-9]+', '_', col).strip('_') for col in cols]
    cols = [f'_{col}' if col and col[0].isdigit() else col for col in cols]
    return ','.join(cols)


def transform_interval_line(line):
    return line.replace('/', ',', 1)


def sort_file(combined_filepath):
    with open(combined_filepath, 'r') as file:
        header = sanitize_header(file.readline())
        lines = file.readlines()
        lines = [transform_interval_line(line) for line in lines]
        sorted_lines = sorted(lines)

    with open(combined_filepath, 'w') as file:
        file.write(header + '\n')
        file.writelines(sorted_lines)


def transform_source_file(storage_client, uri, output_path):
    row_count = 0
    blob = source_blob(storage_client, uri)

    def transform():
        nonlocal row_count
        row_count = 0
        with open(output_path, 'w') as outfile:
            with blob.open('rt') as infile:
                header = infile.readline()
                if not header:
                    raise RuntimeError(f'{uri} is empty')
                outfile.write(sanitize_header(header) + '\n')
                for line in infile:
                    outfile.write(transform_interval_line(line))
                    row_count += 1

    retry(transform, f'transform {uri}')
    return row_count


def parse_table_ref(table_ref):
    parts = table_ref.split('.')
    if len(parts) != 3:
        print(f'Invalid BigQuery table reference "{table_ref}". Expected PROJECT.DATASET.TABLE')
        sys.exit(1)
    return parts


def table_id(project_id, dataset_id, table_id):
    return f'{project_id}.{dataset_id}.{table_id}'


def quoted_table(project_id, dataset_id, table_id):
    return f'`{project_id}.{dataset_id}.{table_id}`'


def quoted_field(name):
    return f'`{name.replace("`", "``")}`'


def load_combined_to_bigquery(client, combined_filepath, table_ref):
    from google.cloud import bigquery

    project_id, dataset_id, target_table_id = parse_table_ref(table_ref)

    dataset = bigquery.Dataset(f'{project_id}.{dataset_id}')
    client.create_dataset(dataset, exists_ok=True)

    table_full_ref = table_id(project_id, dataset_id, target_table_id)
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


def push_to_bigquery(combined_filepath, table_ref):
    from google.cloud import bigquery

    project_id, _, _ = parse_table_ref(table_ref)
    client = bigquery.Client(project=project_id)
    load_combined_to_bigquery(client, combined_filepath, table_ref)


def metadata_table_refs(table_ref):
    project_id, dataset_id, target_table_id = parse_table_ref(table_ref)
    return {
        'project_id': project_id,
        'dataset_id': dataset_id,
        'target_table_id': target_table_id,
        'target': table_id(project_id, dataset_id, target_table_id),
        'index_state': table_id(project_id, dataset_id, f'{target_table_id}__index_state'),
        'processed_files': table_id(project_id, dataset_id, f'{target_table_id}__processed_files'),
        'index_state_table_id': f'{target_table_id}__index_state',
        'processed_files_table_id': f'{target_table_id}__processed_files',
    }


def ensure_incremental_tables(client, table_ref):
    from google.cloud import bigquery

    refs = metadata_table_refs(table_ref)
    dataset = bigquery.Dataset(f'{refs["project_id"]}.{refs["dataset_id"]}')
    client.create_dataset(dataset, exists_ok=True)

    index_state = bigquery.Table(
        refs["index_state"],
        schema=[
            bigquery.SchemaField('publisher_id', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('index_uri', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('generation', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('metageneration', 'STRING'),
            bigquery.SchemaField('updated', 'TIMESTAMP'),
            bigquery.SchemaField('checked_at', 'TIMESTAMP', mode='REQUIRED'),
        ],
    )
    client.create_table(index_state, exists_ok=True)

    processed_files = bigquery.Table(
        refs["processed_files"],
        schema=[
            bigquery.SchemaField('publisher_id', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('source_uri', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('status', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('row_count', 'INTEGER'),
            bigquery.SchemaField('discovered_at', 'TIMESTAMP'),
            bigquery.SchemaField('processed_at', 'TIMESTAMP'),
            bigquery.SchemaField('error', 'STRING'),
        ],
    )
    client.create_table(processed_files, exists_ok=True)
    return refs


def scalar_param(name, value_type, value):
    from google.cloud import bigquery
    return bigquery.ScalarQueryParameter(name, value_type, value)


def array_param(name, value_type, values):
    from google.cloud import bigquery
    return bigquery.ArrayQueryParameter(name, value_type, values)


def run_query(client, query, parameters=None):
    from google.cloud import bigquery

    job_config = bigquery.QueryJobConfig(
        query_parameters=parameters or [],
    )
    return client.query(query, job_config=job_config).result()


def get_index_metadata(storage_client, publisher_id):
    index_uri = index_uri_for_publisher(publisher_id)
    blob = source_blob(storage_client, index_uri)
    retry(blob.reload, f'load metadata for {index_uri}')
    return {
        'index_uri': index_uri,
        'generation': str(blob.generation),
        'metageneration': str(blob.metageneration) if blob.metageneration is not None else None,
        'updated': blob.updated,
    }


def get_stored_index_generation(client, refs, publisher_id, index_uri):
    rows = run_query(
        client,
        f'''
        SELECT generation
        FROM {quoted_table(refs["project_id"], refs["dataset_id"], refs["index_state_table_id"])}
        WHERE publisher_id = @publisher_id AND index_uri = @index_uri
        ORDER BY checked_at DESC
        LIMIT 1
        ''',
        [
            scalar_param('publisher_id', 'STRING', publisher_id),
            scalar_param('index_uri', 'STRING', index_uri),
        ],
    )
    for row in rows:
        return row.generation
    return None


def update_index_state(client, refs, publisher_id, metadata):
    run_query(
        client,
        f'''
        MERGE {quoted_table(refs["project_id"], refs["dataset_id"], refs["index_state_table_id"])} T
        USING (
          SELECT
            @publisher_id AS publisher_id,
            @index_uri AS index_uri,
            @generation AS generation,
            @metageneration AS metageneration,
            @updated AS updated,
            @checked_at AS checked_at
        ) S
        ON T.publisher_id = S.publisher_id AND T.index_uri = S.index_uri
        WHEN MATCHED THEN UPDATE SET
          generation = S.generation,
          metageneration = S.metageneration,
          updated = S.updated,
          checked_at = S.checked_at
        WHEN NOT MATCHED THEN INSERT (
          publisher_id, index_uri, generation, metageneration, updated, checked_at
        ) VALUES (
          S.publisher_id, S.index_uri, S.generation, S.metageneration, S.updated, S.checked_at
        )
        ''',
        [
            scalar_param('publisher_id', 'STRING', publisher_id),
            scalar_param('index_uri', 'STRING', metadata['index_uri']),
            scalar_param('generation', 'STRING', metadata['generation']),
            scalar_param('metageneration', 'STRING', metadata['metageneration']),
            scalar_param('updated', 'TIMESTAMP', metadata['updated']),
            scalar_param('checked_at', 'TIMESTAMP', datetime.now(timezone.utc)),
        ],
    )


def processed_file_count(client, refs, publisher_id):
    rows = run_query(
        client,
        f'''
        SELECT COUNT(*) AS count
        FROM {quoted_table(refs["project_id"], refs["dataset_id"], refs["processed_files_table_id"])}
        WHERE publisher_id = @publisher_id
        ''',
        [scalar_param('publisher_id', 'STRING', publisher_id)],
    )
    return next(iter(rows)).count


def processed_source_uris(client, refs, publisher_id):
    rows = run_query(
        client,
        f'''
        SELECT source_uri
        FROM {quoted_table(refs["project_id"], refs["dataset_id"], refs["processed_files_table_id"])}
        WHERE publisher_id = @publisher_id AND status = 'processed'
        ''',
        [scalar_param('publisher_id', 'STRING', publisher_id)],
    )
    return {row.source_uri for row in rows}


def mark_files_processed(client, refs, publisher_id, source_uris):
    source_uris = list(source_uris)
    if not source_uris:
        return

    run_query(
        client,
        f'''
        MERGE {quoted_table(refs["project_id"], refs["dataset_id"], refs["processed_files_table_id"])} T
        USING (
          SELECT
            @publisher_id AS publisher_id,
            source_uri,
            CURRENT_TIMESTAMP() AS processed_at
          FROM UNNEST(@source_uris) AS source_uri
        ) S
        ON T.publisher_id = S.publisher_id AND T.source_uri = S.source_uri
        WHEN MATCHED THEN UPDATE SET
          status = 'processed',
          row_count = NULL,
          discovered_at = COALESCE(T.discovered_at, S.processed_at),
          processed_at = S.processed_at,
          error = NULL
        WHEN NOT MATCHED THEN INSERT (
          publisher_id, source_uri, status, row_count, discovered_at, processed_at, error
        ) VALUES (
          S.publisher_id, S.source_uri, 'processed', NULL, S.processed_at, S.processed_at, NULL
        )
        ''',
        [
            scalar_param('publisher_id', 'STRING', publisher_id),
            array_param('source_uris', 'STRING', source_uris),
        ],
    )


def mark_file_status(client, refs, publisher_id, source_uri, status, row_count=None, error=None):
    run_query(
        client,
        f'''
        MERGE {quoted_table(refs["project_id"], refs["dataset_id"], refs["processed_files_table_id"])} T
        USING (
          SELECT
            @publisher_id AS publisher_id,
            @source_uri AS source_uri,
            @status AS status,
            @row_count AS row_count,
            @error AS error,
            CURRENT_TIMESTAMP() AS now
        ) S
        ON T.publisher_id = S.publisher_id AND T.source_uri = S.source_uri
        WHEN MATCHED THEN UPDATE SET
          status = S.status,
          row_count = S.row_count,
          discovered_at = COALESCE(T.discovered_at, S.now),
          processed_at = IF(S.status = 'processed', S.now, T.processed_at),
          error = S.error
        WHEN NOT MATCHED THEN INSERT (
          publisher_id, source_uri, status, row_count, discovered_at, processed_at, error
        ) VALUES (
          S.publisher_id,
          S.source_uri,
          S.status,
          S.row_count,
          S.now,
          IF(S.status = 'processed', S.now, NULL),
          S.error
        )
        ''',
        [
            scalar_param('publisher_id', 'STRING', publisher_id),
            scalar_param('source_uri', 'STRING', source_uri),
            scalar_param('status', 'STRING', status),
            scalar_param('row_count', 'INT64', row_count),
            scalar_param('error', 'STRING', error),
        ],
    )


def load_staging_table(client, refs, source_path, staging_table_id):
    from google.cloud import bigquery

    destination = table_id(refs["project_id"], refs["dataset_id"], staging_table_id)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    with open(source_path, 'rb') as f:
        load_job = client.load_table_from_file(f, destination, job_config=job_config)
    load_job.result()
    return client.get_table(destination)


def build_merge_query(refs, staging_table_id, columns):
    key_columns = ['Start', 'End', 'Dataset']
    missing_keys = [column for column in key_columns if column not in columns]
    if missing_keys:
        raise RuntimeError(f'Staging table is missing required key column(s): {", ".join(missing_keys)}')

    update_columns = [column for column in columns if column not in key_columns]
    merge_clauses = []
    if update_columns:
        assignments = ', '.join(
            f'{quoted_field(column)} = S.{quoted_field(column)}' for column in update_columns
        )
        merge_clauses.append(f'WHEN MATCHED THEN UPDATE SET {assignments}')

    insert_columns = ', '.join(quoted_field(column) for column in columns)
    insert_values = ', '.join(f'S.{quoted_field(column)}' for column in columns)
    merge_clauses.append(
        f'WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})'
    )

    join_condition = ' AND '.join(
        f'T.{quoted_field(column)} = S.{quoted_field(column)}' for column in key_columns
    )
    return f'''
    MERGE {quoted_table(refs["project_id"], refs["dataset_id"], refs["target_table_id"])} T
    USING {quoted_table(refs["project_id"], refs["dataset_id"], staging_table_id)} S
    ON {join_condition}
    {' '.join(merge_clauses)}
    '''


def merge_staging_table(client, refs, staging_table_id):
    staging = client.get_table(table_id(refs["project_id"], refs["dataset_id"], staging_table_id))
    columns = [field.name for field in staging.schema]
    query = build_merge_query(refs, staging_table_id, columns)
    run_query(client, query)


def drop_table(client, table_ref):
    from google.api_core import exceptions

    try:
        client.delete_table(table_ref)
    except exceptions.NotFound:
        pass


def ingest_source_file(client, refs, storage_client, publisher_id, source_uri):
    staging_table_id = f'{refs["target_table_id"]}__staging_{int(time.time())}_{uuid.uuid4().hex[:8]}'
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile('w', suffix='.csv', delete=False) as tmp:
            tmp_path = tmp.name

        row_count = transform_source_file(storage_client, source_uri, tmp_path)
        load_staging_table(client, refs, tmp_path, staging_table_id)
        merge_staging_table(client, refs, staging_table_id)
        mark_file_status(client, refs, publisher_id, source_uri, 'processed', row_count=row_count)
        print(f'Ingested {source_uri} ({row_count} row(s))')
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)
        drop_table(client, table_id(refs["project_id"], refs["dataset_id"], staging_table_id))


def bootstrap_incremental(client, refs, publisher_id, storage_client, source_uris, combined_filepath, table_ref):
    print('No processed-file state found; bootstrapping target table from full index')
    combine_files(source_uris, storage_client, combined_filepath)
    sort_file(combined_filepath)
    load_combined_to_bigquery(client, combined_filepath, table_ref)
    mark_files_processed(client, refs, publisher_id, [uri for uri in source_uris if is_stats_uri(uri)])


def run_incremental(publisher_id, storage_client, table_ref, combined_filepath):
    from google.cloud import bigquery

    project_id, _, _ = parse_table_ref(table_ref)
    client = bigquery.Client(project=project_id)
    refs = ensure_incremental_tables(client, table_ref)

    metadata = get_index_metadata(storage_client, publisher_id)
    stored_generation = get_stored_index_generation(
        client, refs, publisher_id, metadata['index_uri']
    )
    processed_count = processed_file_count(client, refs, publisher_id)
    if stored_generation == metadata['generation'] and processed_count > 0:
        update_index_state(client, refs, publisher_id, metadata)
        print(f'Index unchanged at generation {metadata["generation"]}; nothing to ingest')
        return

    source_uris = download_index(publisher_id, storage_client)
    if processed_count == 0:
        bootstrap_incremental(client, refs, publisher_id, storage_client, source_uris, combined_filepath, table_ref)
        update_index_state(client, refs, publisher_id, metadata)
        return

    processed_uris = processed_source_uris(client, refs, publisher_id)
    new_source_uris = [
        uri for uri in source_uris
        if is_stats_uri(uri) and uri not in processed_uris
    ]
    if not new_source_uris:
        update_index_state(client, refs, publisher_id, metadata)
        print('Index changed, but no unprocessed source files were found')
        return

    print(f'Found {len(new_source_uris)} new source file(s)')
    failures = []
    for source_uri in new_source_uris:
        try:
            ingest_source_file(client, refs, storage_client, publisher_id, source_uri)
        except Exception as e:
            failures.append(f'{source_uri}: {e}')
            try:
                mark_file_status(client, refs, publisher_id, source_uri, 'failed', error=str(e))
            except Exception as metadata_error:
                print(f'Failed to mark {source_uri} as failed: {metadata_error}')
            print(f'Failed to ingest {source_uri}: {e}')

    if failures:
        raise RuntimeError(f'{len(failures)} source file(s) failed to ingest')

    update_index_state(client, refs, publisher_id, metadata)


if __name__ == '__main__':
    args = parse_args()
    combined_filepath = f'./data/{args.publisher_id}-combined.csv'

    if args.incremental and not args.bigquery:
        print('--incremental requires --bigquery PROJECT.DATASET.TABLE')
        sys.exit(1)

    with timer('Total run'):
        if not args.no_auth:
            with timer('Checking Google Cloud auth'):
                gcloud_login()

        from google.cloud import storage as gcs

        with timer('Creating GCS client'):
            gcs_client = gcs.Client()

        if args.incremental:
            with timer('Running incremental BigQuery ingestion'):
                run_incremental(args.publisher_id, gcs_client, args.bigquery, combined_filepath)
        else:
            with timer('Loading source index'):
                source_uris = download_index(args.publisher_id, gcs_client)

            with timer('Combining source files'):
                combine_files(source_uris, gcs_client, combined_filepath)

            with timer('Sorting combined CSV'):
                sort_file(combined_filepath)

            if args.bigquery:
                with timer('Loading BigQuery table'):
                    push_to_bigquery(combined_filepath, args.bigquery)
