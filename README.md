# **🚀 Running the API Producer (Pre-Kafka Phase)**

## **Overview**

The `producer.py` script pulls behavioral event data from StackExchange API and writes structured JSON events to the raw data layer.

This sumulates the ingestion stage of the real-time behavioral analytics pipeline before Kafka integration.

---

## **🔧 Prerquisites**
- Python 3.10+
- Virtual environment actiavted
- `.env` file containing:

```
STACKEXCHANGE_API_KEY = your_api_key_here
```

---

## **▶️ How to Run**

From the project root:

```
python producer.py
```

If using the virtual environment explicitly:

```
.\venv\Scripts\python.exe python.py
```