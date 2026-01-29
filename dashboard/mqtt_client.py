import json
import time
from collections import deque

import paho.mqtt.client as mqtt


class MqttBuffer:
    def __init__(self, broker: str, port: int, topic: str, qos: int = 1, maxlen: int = 5000):
        self.broker = broker
        self.port = port
        self.topic = topic
        self.qos = qos
        self.buffer = deque(maxlen=maxlen)
        self.connected = False
        self.last_error = None

        self.client = mqtt.Client(client_id="streamlit_soc_dashboard", clean_session=True)
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.on_disconnect = self._on_disconnect

    def _on_connect(self, client, userdata, flags, rc):
        self.connected = (rc == 0)
        if self.connected:
            client.subscribe(self.topic, qos=self.qos)

    def _on_disconnect(self, client, userdata, rc):
        self.connected = False

    def _on_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
            payload["_received_ts"] = time.time()
            self.buffer.append(payload)
        except Exception as e:
            self.last_error = str(e)

    def start(self):
        try:
            self.client.connect(self.broker, self.port, keepalive=60)
            self.client.loop_start()
        except Exception as e:
            self.last_error = str(e)

    def stop(self):
        try:
            self.client.loop_stop()
            self.client.disconnect()
        except Exception:
            pass
