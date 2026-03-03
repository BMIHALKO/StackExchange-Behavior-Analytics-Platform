import os
import json
import time
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd

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
    "pagesize": 20,
    "order": "desc",
    "sort": "creation",
}

res = requests.get(url, params = params, timeout = 30)
res.raise_for_status()
data = res.json()

items = data.get("items", [])
df = pd.json_normalize(items)

# Few useful columns
keep = [
    "question_id",
    "creation_date",
    "title",
    "score",
    "answer_count",
    "view_account",
    "is_answered",
    "link",
]

existing = [c for c in keep if c in df.columns]
df = df[existing]

# Convert unix time to datetime if present
if "creation_date" in df.columns:
    df["creation_date"] = pd.to_datetime(df["creation_date"], unit = "s")

print(df.head())
print("\nRows:", len(df))
print("Quota remaining:", data.get("quota_remaining"))

# save a small sample
df.to_csv("questions_sample.csv", index = False)
print("Saved: questions_sample.csv")