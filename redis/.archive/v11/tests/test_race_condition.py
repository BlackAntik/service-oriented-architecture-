import threading
import requests
import time
import sys
import os

API_URL = "http://localhost:8000/bookings"
FLIGHT_ID = os.getenv("FLIGHT_ID", "REPLACE_WITH_VALID_FLIGHT_ID")
PASSENGER_NAME_BASE = "Racer"

def book_seat(index):
    payload = {
        "user_id": f"user_{index}",
        "flight_id": FLIGHT_ID,
        "passenger_name": f"{PASSENGER_NAME_BASE}_{index}",
        "passenger_email": f"racer_{index}@example.com",
        "seat_count": 1
    }
    try:
        print(f"Thread {index}: Sending request...")
        response = requests.post(API_URL, json=payload)
        print(f"Thread {index}: Status {response.status_code} - {response.text}")
    except Exception as e:
        print(f"Thread {index}: Error {e}")

def run_race():
    if FLIGHT_ID == "REPLACE_WITH_VALID_FLIGHT_ID":
        print("Error: Please set FLIGHT_ID environment variable or edit the script with a valid flight ID.")
        print("Example: export FLIGHT_ID=your-uuid-here && python3 test_race_condition.py")
        return

    threads = []
    print(f"Starting race for flight {FLIGHT_ID}...")

    for i in range(5):
        t = threading.Thread(target=book_seat, args=(i,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    print("Race finished.")

if __name__ == "__main__":
    run_race()