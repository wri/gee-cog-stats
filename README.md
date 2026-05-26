# GEE COG Stats

If you have created a COG back GEE asset, and requested statistics, this project helps you download them.

## Requirements

- `uv`
- Python 3.9 or newer
- Google Cloud CLI tools, including `gcloud` and `gsutil`
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
2. Download `gs://earthengine-stats/providers/<publisher_id>/index.txt` to `data/<publisher_id>/index.txt`.
3. Download each statistics file listed in `index.txt` into `data/<publisher_id>/`.
4. Combine the downloaded `earthengine_stats*` files into `data/<publisher_id>-combined.csv`.
5. Sort the combined CSV and expand the `Dataset` and `Interval` columns into more specific fields.

If you are not already logged in, the script runs:

```sh
gcloud auth login
```

Downloaded files are skipped when they already exist locally, so it is safe to rerun the script for the same publisher ID.
