import os
import json
import uuid
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

api_key = os.getenv("STACKEXCHANGE_API_KEY")
if not api_key:
    raise RuntimeError("Missing STACKEXCHANGE_API_KEY in .env")

site = os.getenv("STACKEXCHANGE_SITE", "stackoverflow")
pagesize = int(os.getenv("STACKEXCHANGE_PAGESIZE", "100"))
max_pages = int(os.getenv("STACKEXCHANGE_MAX_PAGES", "20"))

kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
kafka_topic = os.getenv("KAFKA_TOPIC", "stackexchange-events")

run_forever = os.getenv("RUN_FOREVER", "false").lower() in ("1", "true", "yes")
max_cycles = int(os.getenv("MAX_CYCLES", "1"))
poll_interval_seconds = float(os.getenv("POLL_INTERVAL_SECONDS", "5"))

url = "https://api.stackexchange.com/2.3/questions"
base_params = {
    "site": site,
    "key": api_key,
    "pagesize": pagesize,
    "order": "desc",
    "sort": "creation",
}


def unix_to_iso_utc(unix_ts: int | None) -> str:
    """Convert UNIX seconds to ISO 8601 UTC string."""
    if unix_ts is None:
        return datetime.now(timezone.utc).isoformat()
    return datetime.fromtimestamp(unix_ts, tz = timezone.utc).isoformat()


def build_event(item: dict) -> dict:
    """Build the locked event schema for Kafka publishing."""
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

def publish_one_cycle(producer: KafkaProducer) -> tuple[int, dict | None]:
    """
    Publish one full cycle of API pulls.
    A cycle = page 1 through max_pages (or until API says has_more is false)
    """
    total_events_sent = 0
    last_response_data: dict | None = None

    for page in range(1, max_pages + 1):
        params_with_page = dict(base_params)
        params_with_page["page"] = page

        res = requests.get(url, params = params_with_page, timeout = 30)
        res.raise_for_status()
        data = res.json()
        last_response_data = data

        items = data.get("items", [])

        for item in items:
            event = build_event(item)
            producer.send(
                kafka_topic,
                key = event["event_id"],
                value = event,
            )
            total_events_sent += 1

        # Push messages for this page
        producer.flush()

        backoff = data.get("backoff")
        if backoff is not None:
            print(f"API requested backoff: sleeping {int(backoff)} second(s)")
            time.sleep(int(backoff))
        
        if not data.get("has_more", False):
            break
        
        time.sleep(0.25)

    return total_events_sent, last_response_data

def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers = "localhost:9094",
        value_serializer = lambda v: json.dumps(v).encode("utf-8"),
        key_serializer = lambda k: k.encode("utf-8") if k is not None else None,
    )

    cycle_number = 1
    grand_total = 0

    try:
        while True:
            print(f"\nStarting Producer Cycle {cycle_number}...")

            total_events_sent, last_response_data = publish_one_cycle(producer)
            grand_total += total_events_sent

            if total_events_sent == 0:
                print(f"Cycle {cycle_number}: no items returned from API")
            else:
                print(
                    f"Cycle {cycle_number}: published {total_events_sent} events "
                    f"to Kafka topic '{kafka_topic}'."
                )

            quota_remaining = (
                None if last_response_data is None else last_response_data.get("quota_remaining")
            )
            print(f"Quota remaining: {quota_remaining}")
            print(f"Grand total published so far: {grand_total}")

            if not run_forever and cycle_number >= max_cycles:
                print(f"Reached MAX_CYCLES = {max_cycles}. Exiting producer.")
                break
            
            if run_forever:
                print(
                    f"RUN_FOREVER = true. Sleeping {poll_interval_seconds} second(s) "
                    f"before next cycle..."
                )
            else:
                print(
                    f"Sleeping {poll_interval_seconds} second(s) before cycle "
                    f"{cycle_number + 1} of {max_cycles}..."
                )

            time.sleep(poll_interval_seconds)
            cycle_number += 1
    
    except KeyboardInterrupt:
        print("\nProducer stopped manually.")

    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()