#!/bin/bash
set -e

PROJECT_ID=$(gcloud config get-value project)
ZONE="us-central1-a"
REGION="us-central1"
BUCKET="bu-cs528-mahicm13"

echo "==> Deleting VMs..."
gcloud compute instances delete hw4-webserver --zone=${ZONE} --quiet 2>/dev/null || true
gcloud compute instances delete hw4-client --zone=${ZONE} --quiet 2>/dev/null || true
gcloud compute instances delete hw4-reporter --zone=${ZONE} --quiet 2>/dev/null || true

echo "==> Releasing static IP..."
gcloud compute addresses delete hw4-webserver-ip --region=${REGION} --quiet 2>/dev/null || true

echo "==> Deleting firewall rules..."
gcloud compute firewall-rules delete allow-webserver-8080 --quiet 2>/dev/null || true
gcloud compute firewall-rules delete allow-reporter-9090 --quiet 2>/dev/null || true

echo "==> Removing scripts from GCS..."
gsutil -m rm -r gs://${BUCKET}/hw4-scripts/ 2>/dev/null || true

echo "==> Deleting service accounts..."
gcloud iam service-accounts delete hw4-webserver-sa@${PROJECT_ID}.iam.gserviceaccount.com --quiet 2>/dev/null || true
gcloud iam service-accounts delete hw4-reporter-sa@${PROJECT_ID}.iam.gserviceaccount.com --quiet 2>/dev/null || true

echo "✅ Cleanup complete."