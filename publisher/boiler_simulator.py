import json
import time
import random
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

from config import (
    BROKER, PORT, TOPIC, QOS, DEVICE_ID, PUBLISH_INTERVAL_SEC,
    TEMP_MIN, TEMP_MAX, PRESSURE_MIN, PRESSURE_MAX
)

import sys
import os

# Import integrity module from dashboard folder
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "dashboard")))
from integrity import sha256_hash


def compute_status(temp_c: float, pressure_psi: float) -> str:
    if temp_c >= 95.0 or pressure_psi >= 45.0:
        return "Critical"
    if temp_c >= 85.0 or pressure_psi >= 35.0:
        return "Warning"
    return "OK"


def main():
    client = mqtt.Client(client_id=f"{DEVICE_ID}_publisher", clean_session=True)
    client.connect(BROKER, PORT, keepalive=60)
    client.loop_start()

    temp = random.uniform(TEMP_MIN, TEMP_MAX)
    pressure = random.uniform(PRESSURE_MIN, PRESSURE_MAX)

    print(f"Publishing to broker={BROKER}, topic={TOPIC}")
    try:
        while True:
            # Smooth-ish drift + noise
            temp += random.uniform(-0.6, 0.8)
            temp = max(50.0, min(110.0, temp))

            # Pressure follows temperature slightly + noise
            pressure += (temp - 75.0) * 0.01 + random.uniform(-0.4, 0.4)
            pressure = max(10.0, min(55.0, pressure))

            status = compute_status(temp, pressure)

            payload = {
                "device_id": DEVICE_ID,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "temperature": round(temp, 2),
                "pressure": round(pressure, 2),
                "status": status
            }

            payload["hash"] = sha256_hash(payload)

            client.publish(TOPIC, json.dumps(payload), qos=QOS, retain=False)

            print(payload)
            time.sleep(PUBLISH_INTERVAL_SEC)

    except KeyboardInterrupt:
        print("\nStopping publisher...")

    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
