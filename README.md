# GEE COG Stats

If you have created a COG back GEE asset, and requested statistics, this project helps you download them.

## Requirements

- `uv`
- Python 3.9 or newer
- Google Cloud CLI tools for local authentication and deployment
- Access to `gs://earthengine-stats/providers/<publisher_id>/`

## Usage

Run the download script from the repository root with your publisher ID:

```sh
uv run download.py <publisher_id>
```

For example:

```sh
uv run download.py landandcarbon
```

Because `download.py` includes a uv shebang, you can also run it directly:

```sh
./download.py <publisher_id>
```

The script will:

1. Check that you are logged in to Google Cloud.
2. Read `gs://earthengine-stats/providers/<publisher_id>/index.txt` and save a copy to `data/<publisher_id>/index.txt`.
3. Stream each listed `earthengine_stats*` file from GCS into `data/<publisher_id>-combined.csv`.
4. Sort the combined CSV and expand the `Dataset` and `Interval` columns into more specific fields.

If you are not already logged in, the script runs:

```sh
gcloud auth login
```

The script rebuilds the combined CSV from the current GCS index on each run, so it is safe to rerun for the same publisher ID.

## Pushing to BigQuery

Pass `--bigquery PROJECT.DATASET.TABLE` to load the combined CSV into a BigQuery table after processing. The dataset and table are created automatically if they do not already exist. Re-running overwrites the table with the latest data.

```sh
uv run download.py landandcarbon --bigquery landandcarbon.gee_cog_stats.30_day_active_users
```

The BigQuery client uses your active `gcloud` credentials, so no additional authentication is required beyond the login step.

## Running on a schedule

`deploy-cloudrun.sh` builds a Docker image, creates a Cloud Run Job, and sets up a Cloud Scheduler trigger. Run it once to deploy; re-run to update the image or schedule.

```sh
GCP_PROJECT=<your-gcp-project> \
PUBLISHER_ID=<publisher-id> \
BIGQUERY_TABLE=<your-gcp-project>.<dataset>.<table> \
./deploy-cloudrun.sh
```

For example, to deploy for Land & Carbon:

```sh
GCP_PROJECT=landandcarbon \
PUBLISHER_ID=landandcarbon \
BIGQUERY_TABLE=landandcarbon.gee_cog_stats.30_day_active_users \
./deploy-cloudrun.sh
```

The script creates all required GCP resources (Artifact Registry repo, service account, IAM bindings, Cloud Run Job, Cloud Scheduler job). To run the job immediately after deploying:

```sh
gcloud run jobs execute gee-cog-stats --region us-central1 --project landandcarbon
```

Set `REGION` or `SCHEDULE` environment variables to override the defaults (`us-central1` and `0 6 * * *`). Run the deploy script once per publisher to set up independent jobs for each.
