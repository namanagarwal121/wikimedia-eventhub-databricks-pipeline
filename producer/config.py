import os
from dotenv import load_dotenv

load_dotenv()

EVENT_HUB_CONN_STR = os.getenv("EVENT_HUB_CONN_STR")
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME")

WIKIMEDIA_URL = "https://stream.wikimedia.org/v2/stream/recentchange"

HEADER = {
    "User-Agent": "wikimedia-streaming-pipeline/1.0"
}
