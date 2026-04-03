import requests
import logging
import signal
import sys
import time
from azure.eventhub import EventHubProducerClient, EventData

from config import EVENT_HUB_CONN_STR, EVENT_HUB_NAME, WIKIMEDIA_URL, HEADER

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

def validate_config():
    if not EVENT_HUB_CONN_STR:
        raise ValueError("Missing EVENT_HUB_CONN_STR")
    if not EVENT_HUB_NAME:
        raise ValueError("Missing EVENT_HUB_NAME")

shutdown_flag = False

def handle_shutdown(signum, frame):
    global shutdown_flag
    logger.info("Shutdown signal received...")
    shutdown_flag = True

signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)

def send_with_retry(producer, batch, retries=3, delay=2):
    for attempt in range(1, retries + 1):
        try:
            producer.send_batch(batch)
            return True
        except Exception as e:
            logger.error(f"Send failed (attempt {attempt}): {e}")
            time.sleep(delay)

    logger.error("Failed to send batch after retries")
    return False

def run():
    validate_config()

    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONN_STR,
        eventhub_name=EVENT_HUB_NAME
    )

    logger.info("Producer started...")

    response = requests.get(WIKIMEDIA_URL, stream=True, headers=HEADER)

    batch = producer.create_batch()
    event_count = 0
    total_events = 0

    try:
        for line in response.iter_lines():
            if shutdown_flag:
                logger.info("Shutting down gracefully...")
                break

            if not line:
                continue

            decoded_line = line.decode("utf-8")

            if not decoded_line.startswith("data:"):
                continue

            json_data = decoded_line.replace("data: ", "")

            try:
                batch.add(EventData(json_data))
                event_count += 1
                total_events += 1

            except ValueError:
                if send_with_retry(producer, batch):
                    logger.info(f"Sent batch with {event_count} events")

                batch = producer.create_batch()
                batch.add(EventData(json_data))
                event_count = 1

        if event_count > 0:
            if send_with_retry(producer, batch):
                logger.info(f"Final batch sent with {event_count} events")

    except Exception as e:
        logger.exception(f"Unexpected error: {e}")

    finally:
        producer.close()
        logger.info(f"Producer stopped. Total events sent: {total_events}")

if __name__ == "__main__":
    run()