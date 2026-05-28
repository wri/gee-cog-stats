#!/usr/bin/env bash
# Deploy download.py as a Cloud Run Job triggered daily by Cloud Scheduler.
# Run once to set up; re-run to update the image or schedule.
#
# Required env vars (or edit defaults below):
#   GCP_PROJECT      – your GCP project ID
#   PUBLISHER_ID     – publisher to download (required, e.g. landandcarbon)
#   BIGQUERY_TABLE   – optional, PROJECT.DATASET.TABLE
#   REGION           – Cloud Run region (default: us-central1)
#   SCHEDULE         – cron expression (default: daily at 6 AM UTC)

set -euo pipefail

PROJECT_ID="${GCP_PROJECT:?Set GCP_PROJECT to your project ID}"
PUBLISHER_ID="${PUBLISHER_ID:?Set PUBLISHER_ID to the publisher to download (e.g. landandcarbon)}"
BIGQUERY_TABLE="${BIGQUERY_TABLE:-}"
REGION="${REGION:-us-central1}"
SCHEDULE="${SCHEDULE:-0 6 * * *}"
SA_NAME="gee-stats-runner"
JOB_NAME="gee-cog-stats"
IMAGE="$REGION-docker.pkg.dev/$PROJECT_ID/gee-cog-stats/downloader"
SCHEDULER_JOB="$JOB_NAME-daily"

# Build job args list
JOB_ARGS="$PUBLISHER_ID,--no-auth"
if [ -n "$BIGQUERY_TABLE" ]; then
  JOB_ARGS="$JOB_ARGS,--bigquery,$BIGQUERY_TABLE"
fi

echo "==> Project:   $PROJECT_ID"
echo "==> Region:    $REGION"
echo "==> Publisher: $PUBLISHER_ID"
echo "==> Schedule:  $SCHEDULE"
[ -n "$BIGQUERY_TABLE" ] && echo "==> BigQuery:  $BIGQUERY_TABLE"
echo ""

# --- 1. Artifact Registry repo ---
echo "==> Creating Artifact Registry repository (if needed)..."
gcloud artifacts repositories create gee-cog-stats \
  --repository-format=docker \
  --location="$REGION" \
  --project="$PROJECT_ID" 2>/dev/null || true

# --- 2. Service account ---
echo "==> Creating service account (if needed)..."
gcloud iam service-accounts create "$SA_NAME" \
  --display-name="GEE COG Stats runner" \
  --project="$PROJECT_ID" 2>/dev/null || true

SA_EMAIL="$SA_NAME@$PROJECT_ID.iam.gserviceaccount.com"

# Grant permissions the job needs
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/storage.objectViewer" --condition=None --quiet --format=none

if [ -n "$BIGQUERY_TABLE" ]; then
  gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/bigquery.dataEditor" --condition=None --quiet --format=none
  gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/bigquery.jobUser" --condition=None --quiet --format=none
fi

# Cloud Scheduler needs permission to invoke Cloud Run Jobs
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/run.invoker" --condition=None --quiet --format=none

# --- 3. Build and push image ---
echo "==> Building and pushing Docker image..."
gcloud builds submit \
  --tag "$IMAGE" \
  --project="$PROJECT_ID"

# --- 4. Cloud Run Job ---
echo "==> Deploying Cloud Run Job..."
gcloud run jobs deploy "$JOB_NAME" \
  --image "$IMAGE" \
  --region "$REGION" \
  --service-account "$SA_EMAIL" \
  --args "$JOB_ARGS" \
  --max-retries 1 \
  --task-timeout 1200 \
  --project="$PROJECT_ID"

# --- 5. Cloud Scheduler ---
echo "==> Configuring Cloud Scheduler..."
JOB_URI="https://$REGION-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/$PROJECT_ID/jobs/$JOB_NAME:run"

SCHEDULER_ARGS=(
  --location="$REGION"
  --schedule="$SCHEDULE"
  --uri="$JOB_URI"
  --message-body='{}'
  --oauth-service-account-email="$SA_EMAIL"
  --project="$PROJECT_ID"
)

if gcloud scheduler jobs describe "$SCHEDULER_JOB" --location="$REGION" --project="$PROJECT_ID" &>/dev/null; then
  gcloud scheduler jobs update http "$SCHEDULER_JOB" "${SCHEDULER_ARGS[@]}"
else
  gcloud scheduler jobs create http "$SCHEDULER_JOB" "${SCHEDULER_ARGS[@]}"
fi

echo ""
echo "Done. To run immediately:"
echo "  gcloud run jobs execute $JOB_NAME --region $REGION --project $PROJECT_ID"
