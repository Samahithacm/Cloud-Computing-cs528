#!/bin/bash
set -e

PROJECT_ID=$(gcloud config get-value project)
REGION="us-central1"
ZONE="us-central1-a"
BUCKET="bu-cs528-mahicm13"

echo "==> Enabling required APIs..."
gcloud services enable compute.googleapis.com logging.googleapis.com storage.googleapis.com

echo "==> Creating service accounts..."
gcloud iam service-accounts create hw4-webserver-sa \
  --display-name="HW4 Web Server Service Account" 2>/dev/null || echo "Already exists"

gcloud iam service-accounts create hw4-reporter-sa \
  --display-name="HW4 Reporter Service Account" 2>/dev/null || echo "Already exists"

echo "==> Granting IAM permissions..."
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:hw4-webserver-sa@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/storage.objectViewer"

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:hw4-webserver-sa@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/logging.logWriter"

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:hw4-reporter-sa@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/logging.logWriter"

echo "==> Reserving static IP..."
gcloud compute addresses create hw4-webserver-ip \
  --region=${REGION} 2>/dev/null || echo "Already exists"

STATIC_IP=$(gcloud compute addresses describe hw4-webserver-ip \
  --region=${REGION} --format="get(address)")
echo "Static IP: ${STATIC_IP}"

echo "==> Uploading scripts to GCS..."
gsutil cp server.py gs://${BUCKET}/hw4-scripts/server.py
gsutil cp reporter.py gs://${BUCKET}/hw4-scripts/reporter.py

echo "==> Creating VM3 (Reporter)..."
gcloud compute instances create hw4-reporter \
  --zone=${ZONE} \
  --machine-type=e2-micro \
  --service-account=hw4-reporter-sa@${PROJECT_ID}.iam.gserviceaccount.com \
  --scopes=https://www.googleapis.com/auth/logging.write,https://www.googleapis.com/auth/devstorage.read_only \
  --metadata-from-file=startup-script=startup-reporter.sh \
  --tags=reporter-server

sleep 15
REPORTER_IP=$(gcloud compute instances describe hw4-reporter \
  --zone=${ZONE} --format="get(networkInterfaces[0].networkIP)")
echo "Reporter internal IP: ${REPORTER_IP}"

echo "==> Creating VM1 (Web Server)..."
gcloud compute instances create hw4-webserver \
  --zone=${ZONE} \
  --machine-type=e2-micro \
  --service-account=hw4-webserver-sa@${PROJECT_ID}.iam.gserviceaccount.com \
  --scopes=https://www.googleapis.com/auth/cloud-platform \
  --address=${STATIC_IP} \
  --metadata=reporter-ip=${REPORTER_IP} \
  --metadata-from-file=startup-script=startup.sh \
  --tags=web-server

echo "==> Creating VM2 (Client)..."
gcloud compute instances create hw4-client \
  --zone=${ZONE} \
  --machine-type=e2-micro \
  --scopes=https://www.googleapis.com/auth/cloud-platform

echo "==> Creating firewall rules..."
gcloud compute firewall-rules create allow-webserver-8080 \
  --allow=tcp:8080 \
  --target-tags=web-server \
  --source-ranges=0.0.0.0/0 2>/dev/null || echo "Already exists"

gcloud compute firewall-rules create allow-reporter-9090 \
  --allow=tcp:9090 \
  --target-tags=reporter-server \
  --source-tags=web-server 2>/dev/null || echo "Already exists"

echo ""
echo "✅ Setup complete!"
echo "   Web server static IP: ${STATIC_IP}"
echo "   Run client with: python3 client.py ${STATIC_IP}"