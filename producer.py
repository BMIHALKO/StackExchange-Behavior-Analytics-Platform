import os
import json
import uuid
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("STACKEXCHANGE_API_KEY")
if not api_key:
    raise RuntimeError("Missing STACKEXCHANGE_API_KEY in .env")

site = os.getenv("STACKEXCHANGE_SITE", "stackoverflow")
pagesize = int(os.getenv("STACKEXCHANGE_PAGESIZE", "100"))
max_pages = int(os.getenv("STACKEXCHANGE_MAX_PAGES", "5"))

url = "https://api.stackexchange.com/2.3/questions"
params = {
    "site": site,
    "key": api_key,
    "pagesize": pagesize,
    "order": "desc",
    "sort": "creation",
}

# Write into the spec folder location
out_dir = Path(os.getenv("RAW_EVENTS_DIR", "data/raw"))
run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
out_path = out_dir / f"date={run_date}" / "behavior_events_sample.ndjson"

# Converts UNIX seconds -> UTC string
def unix_to_iso_utc(unix_ts: int | None) -> str:    
    if unix_ts is None:
        return datetime.now(timezone.utc).isoformat()
    return datetime.fromtimestamp(unix_ts, tz = timezone.utc).isoformat()

def build_event(item: dict) -> dict:
    # Building event schema
    owner = item.get("owner") or {}
    user_id = owner.get("user_id")
    
    if user_id is None:
        user_id = f"anon-{item.get('question_id', 'unknown')}"
    
    payload = {
        "question_id": item.get("question_id"),
        "creation_date": item.get("creation_date"),
        "title": item.get("title"),
        "score": item.get("score"),
        "answer_count": item.get("answer_count"),
        "is_answered": item.get("is_answered"),
        "link": item.get("link"),
    }

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "SOCIAL_INTERACTION",
        "timestamp": unix_to_iso_utc(item.get("creation_date")),
        "user_id": str(user_id),
        "region": "UNKNOWN",
        "source": "stackexchange_api",
        "payload_version": 1,
        "payload": payload,
    }

def main() -> None:
    out_path.parent.mkdir(parents = True, exist_ok = True)

    all_items: list[dict] = []
    last_response_data: dict | None = None

    for page in range(1, max_pages + 1):
        params_with_page = dict(params)
        params_with_page["page"] = page

        res = requests.get(url, params = params_with_page, timeout = 30)
        res.raise_for_status()
        data = res.json()
        last_response_data = data

        items = data.get("items", [])
        all_items.extend(items)

        backoff = data.get("backoff")
        if backoff is not None:
            time.sleeip(int(backoff))
        
        # Stop early if there are no more pages
        if not data.get("has_more", False):
            break

        time.sleep(0.25)
    
    if not all_items:
        print("No items returned from API.")
    
    with out_path.open("w", encoding = "utf-8") as f:
        for item in all_items:
            event = build_event(item)
            f.write(json.dumps(event, ensure_ascii = False) + "\n")
    
    quota_remaining = None if last_response_data is None else last_response_data.get("quota_remaining")
    print(f"Wrote {len(all_items)} events to {out_path}")
    print(f"Quota remaining: {quota_remaining}")

if __name__ == "__main__":
    main()