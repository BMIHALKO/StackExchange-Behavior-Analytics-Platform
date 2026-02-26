import os
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("STACKEXCHANGE_API_KEY")
if not api_key:
    raise RuntimeError("Missing STACKEXCHANGE_API_KEY in .env")

url = "https://api.stackexchange.com/2.3/questions"
params = {
    "site": "stackoverflow",
    "key": api_key,
    "pagesize": 5,
    "order": "desc",
    "sort": "creation",
}

res = requests.get(url, params=params, timeout=30)
print("Status:", res.status_code)

data = res.json()
print("Items:", len(data.get("items", [])))
print("Quota remaining:", data.get("quota_remaining"))

# Print the first title so we know it's real data
items = data.get("items", [])
if items:
    print("Sample title:", items[0].get("title"))