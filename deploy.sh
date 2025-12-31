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
    --set-env-vars="DB_CONNECTION_NAME=wandern-project-startup:us-central1:wandern-postgres,DB_USER=wandern_app,DB_NAME=wandern_db" \
    --project=$PROJECT_ID

echo "✅ Deploy complete!"
echo "URL: https://$REGION-$PROJECT_ID.cloudfunctions.net/$FUNCTION_NAME"
