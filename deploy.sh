#!/bin/bash
# Deploy Arweave Uploader Cloud Function

set -e

PROJECT_ID="wandern-project-startup"
REGION="us-central1"
FUNCTION_NAME="arweave-uploader"

echo "🚀 Deploying $FUNCTION_NAME..."

gcloud functions deploy $FUNCTION_NAME \
    --gen2 \
    --runtime=python311 \
    --region=$REGION \
    --source=. \
    --entry-point=upload_batch \
    --trigger-http \
    --allow-unauthenticated \
    --memory=512MB \
    --timeout=300s \
    --set-env-vars="INSTANCE_CONNECTION_NAME=wandern-project-startup:us-central1:wandern-postgres-instance-v3,DB_USER=wandern_user,DB_PASSWORD=Role7442,DB_NAME=wandern" \
    --project=$PROJECT_ID

echo "✅ Deploy complete!"
echo "URL: https://$REGION-$PROJECT_ID.cloudfunctions.net/$FUNCTION_NAME"
