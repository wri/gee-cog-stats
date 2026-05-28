#!/usr/bin/env bash
# Deploy download.py as a Cloud Run Job triggered daily by Cloud Scheduler.
# Run once to set up; re-run to update the image or schedule.
#
# Required env vars (or edit defaults below):
#   GCP_PROJECT      – your GCP project ID
#   PUBLISHER_ID     – publisher to download (required, e.g. landandcarbon)
#   BIGQUERY_TABLE   – BigQuery target table, PROJECT.DATASET.TABLE
#   EXTRA_LABELS     – optional extra labels, KEY=VALUE,...
#   REGION           – Cloud Run region (default: us-central1)
#   SCHEDULE         – cron expression (default: daily at 6 AM UTC)

set -euo pipefail

PROJECT_ID="${GCP_PROJECT:?Set GCP_PROJECT to your project ID}"
PUBLISHER_ID="${PUBLISHER_ID:?Set PUBLISHER_ID to the publisher to download (e.g. landandcarbon)}"
BIGQUERY_TABLE="${BIGQUERY_TABLE:?Set BIGQUERY_TABLE to PROJECT.DATASET.TABLE}"
REGION="${REGION:-us-central1}"
SCHEDULE="${SCHEDULE:-0 6 * * *}"
SA_NAME="gee-stats-runner"
JOB_NAME="gee-cog-stats"
IMAGE="$REGION-docker.pkg.dev/$PROJECT_ID/gee-cog-stats/downloader"
SCHEDULER_JOB="$JOB_NAME-daily"
BASE_LABELS="app=$JOB_NAME,publisher=$PUBLISHER_ID,managed_by=deploy-cloudrun"
RESOURCE_LABELS="$BASE_LABELS"
if [ -n "${EXTRA_LABELS:-}" ]; then
  RESOURCE_LABELS="$RESOURCE_LABELS,$EXTRA_LABELS"
fi

validate_labels() {
  local labels="$1"
  local pair key value
  IFS=',' read -ra pairs <<< "$labels"
  for pair in "${pairs[@]}"; do
    key="${pair%%=*}"
    value="${pair#*=}"
    if [[ "$pair" != *=* || ! "$key" =~ ^[a-z][a-z0-9_-]{0,62}$ || ! "$value" =~ ^[a-z0-9_-]{0,63}$ ]]; then
      echo "Invalid label '$pair'. Labels must be KEY=VALUE with lowercase letters, digits, hyphens, or underscores."
      exit 1
    fi
  done
}

validate_labels "$RESOURCE_LABELS"

# Build job args list
JOB_ARGS="$PUBLISHER_ID,--no-auth,--incremental,--bigquery,$BIGQUERY_TABLE"

echo "==> Project:   $PROJECT_ID"
echo "==> Region:    $REGION"
echo "==> Publisher: $PUBLISHER_ID"
echo "==> Schedule:  $SCHEDULE"
echo "==> BigQuery:  $BIGQUERY_TABLE"
echo "==> Labels:    $RESOURCE_LABELS"
echo ""

# --- 1. Artifact Registry repo ---
echo "==> Creating/updating Artifact Registry repository..."
if gcloud artifacts repositories describe gee-cog-stats --location="$REGION" --project="$PROJECT_ID" &>/dev/null; then
  gcloud artifacts repositories update gee-cog-stats \
    --location="$REGION" \
    --update-labels="$RESOURCE_LABELS" \
    --project="$PROJECT_ID"
else
  gcloud artifacts repositories create gee-cog-stats \
    --repository-format=docker \
    --location="$REGION" \
    --labels="$RESOURCE_LABELS" \
    --project="$PROJECT_ID"
fi

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

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/bigquery.dataOwner" --condition=None --quiet --format=none
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/bigquery.jobUser" --condition=None --quiet --format=none

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
  --labels "$RESOURCE_LABELS" \
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
