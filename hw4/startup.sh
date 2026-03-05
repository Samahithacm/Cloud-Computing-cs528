#!/bin/bash

if [ -f /var/log/startup_already_done ]; then
    echo "Startup already ran. Skipping."
    exit 0
fi

apt-get update -y
apt-get install -y python3 python3-pip curl

pip3 install --break-system-packages google-cloud-storage google-cloud-logging requests

gsutil cp gs://bu-cs528-mahicm13/hw4-scripts/server.py /home/server.py

REPORTER_IP=$(curl -sf "http://metadata.google.internal/computeMetadata/v1/instance/attributes/reporter-ip" -H "Metadata-Flavor: Google")

sed -i "s/REPORTER_IP_PLACEHOLDER/${REPORTER_IP}/" /home/server.py

nohup python3 /home/server.py > /var/log/webserver.log 2>&1 &

touch /var/log/startup_already_done
echo "Web server started."