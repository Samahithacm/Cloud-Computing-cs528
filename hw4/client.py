#!/usr/bin/env python3

import requests
import sys
import random

SERVER_IP = sys.argv[1]
PORT = 8080

# Pick 100 random files from your bucket (files are named 1.html to 9999+)
file_ids = random.sample(range(1, 9000), 100)
files = [f"html_files/{i}.html" for i in file_ids]

for f in files:
    url = f"http://{SERVER_IP}:{PORT}/{f}"
    try:
        r = requests.get(url, timeout=5)
        print(f"{r.status_code} — {f}")
    except Exception as e:
        print(f"ERROR — {f} — {e}")