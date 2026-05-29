# MIT License
#
# Copyright (c) 2026 Patrick Yiu
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
# Edit history
# 2026/04/15 - PY - refactored for Eversource electricity stats, which report meter readings at 15-minute intervals
# 2026/04/15 - PY - comments
# 2026/05/14 - PY - refactor to prevent occasional negative daily readings. Force UTC time for comparisons.
# 2026/05/14 - PY - use single source of truth for "last_sum": trust last_sum from previous entity state and use as basis for both live state and LTS import.
# 2026/05/22 - PY - changed the live entity's state_class from "total_increasing" to "total" to prevent desync between Home Assistant's state engine and this script's cumulative sum.
# 2026/05/22 - PY - explicitly set daily refresh/fetch time as 1400 Eastern Time
# 2026/05/28 - PY - correctly extract and accumulate ALL available API data intervals instead of just the first
# 2026/05/28 - PY - refactor to automatically create new device + child sensor(s) via MQTT discovery
# 2026/05/28 - PY - refactor to extensibly support additional child sensors via SENSORS_CONFIG dictionary

import json
import requests
import websocket
from datetime import datetime, timedelta, UTC
from zoneinfo import ZoneInfo
import appdaemon.plugins.hass.hassapi as hass

# UtilityAPI configuration
ACCESS_TOKEN = "PLACEHOLDER" # Update to access token from UtilityAPI
METER_ID = "PLACEHOLDER" # Update to meter ID from UtilityAPI

# Home Assistant configuration
HA_URL = "ws://homeassistant.local:1234/api/websocket"  # Update to your HA WebSocket URL

SCRIPT_VERSION = "1.0"
MAX_DAYS = 365
LAST_TIME_ATTR_STR = "last_fetched_interval"

SENSORS_CONFIG = {
    "net_energy": {
        "name": "Energy Usage (Net)",
        "entity_id": "sensor.utilityapi_eversource_energy_usage_net",
        "state_topic": "homeassistant/sensor/eversource_usage/net_energy/state",
        "attributes_topic": "homeassistant/sensor/eversource_usage/net_energy/attributes",
        "discovery_topic": "homeassistant/sensor/eversource_usage/net_energy/config",
        "unit": "kWh",
        "device_class": "energy",
        "state_class": "measurement",
        "datapoint_type": "net"  # Extracts 'net' type from JSON
    },
    "fwd_energy": {
        "name": "Energy Usage (Forward)",
        "entity_id": "sensor.utilityapi_eversource_energy_usage_fwd",
        "state_topic": "homeassistant/sensor/eversource_usage/fwd_energy/state",
        "attributes_topic": "homeassistant/sensor/eversource_usage/fwd_energy/attributes",
        "discovery_topic": "homeassistant/sensor/eversource_usage/fwd_energy/config",
        "unit": "kWh",
        "device_class": "energy",
        "state_class": "measurement",
        "datapoint_type": "fwd"  # Extracts 'fwd' type from JSON
    },
    # DISABLE peak_demand. It's not useful because API updates come only once per day and only the last value from that pull can be shown.
    # "peak_demand": {
    #     "name": "Peak Demand",
    #     "entity_id": "sensor.utilityapi_eversource_peak_demand",
    #     "state_topic": "homeassistant/sensor/eversource_usage/peak_demand/state",
    #     "attributes_topic": "homeassistant/sensor/eversource_usage/peak_demand/attributes",
    #     "discovery_topic": "homeassistant/sensor/eversource_usage/peak_demand/config",
    #     "unit": "kW",
    #     "device_class": "power",
    #     "state_class": "measurement",
    #     "datapoint_type": "max"  # Extracts 'max' type from JSON
    # },
    "fwd_energy_cumulative_monotonic": {
        "name": "Energy Usage (Forward, Cumulative Monotonic)",
        "entity_id": "sensor.utilityapi_eversource_energy_usage_fwd_cumulative_monotonic",
        "state_topic": "homeassistant/sensor/eversource_usage/fwd_energy_cumulative_monotonic/state",
        "attributes_topic": "homeassistant/sensor/eversource_usage/fwd_energy_cumulative_monotonic/attributes",
        "discovery_topic": "homeassistant/sensor/eversource_usage/fwd_energy_cumulative_monotonic/config",
        "unit": "kWh",
        "device_class": "energy",
        "state_class": "total_increasing",
        "datapoint_type": "fwd"  # Extracts 'fwd' type from JSON
    },
    "fwd_energy_cumulative_nonmonotonic": {
        "name": "Energy Usage (Forward, Cumulative Non-Monotonic)",
        "entity_id": "sensor.utilityapi_eversource_energy_usage_fwd_cumulative_nonmonotonic",
        "state_topic": "homeassistant/sensor/eversource_usage/fwd_energy_cumulative_nonmonotonic/state",
        "attributes_topic": "homeassistant/sensor/eversource_usage/fwd_energy_cumulative_nonmonotonic/attributes",
        "discovery_topic": "homeassistant/sensor/eversource_usage/fwd_energy_cumulative_nonmonotonic/config",
        "unit": "kWh",
        "device_class": "energy",
        "state_class": "total",
        "datapoint_type": "fwd"  # Extracts 'fwd' type from JSON
    },
}

class UtilityAPIDataImporter(hass.Hass):
    def initialize(self):
        # 1. Setup MQTT Discovery via HA Core to create the Device and Sensor in EMQX
        self.setup_mqtt_device()

        # Run daily at 2:00 PM Eastern Time
        eastern_tz = ZoneInfo("America/New_York")
        self.run_daily(self.fetch_and_import_utilityapi_data, "14:00:00", target_tz=eastern_tz)

        # Run once on startup
        self.fetch_and_import_utilityapi_data({})

    def setup_mqtt_device(self):
        """
        Publishes an MQTT Discovery payload(s) through Home Assistant's native
        MQTT service layer. This auto-provisions the device and its child sensor(s) underneath.
        """
        for _, cfg in SENSORS_CONFIG.items():
            payload = {
                "name": cfg["name"],
                "unique_id": cfg["entity_id"],
                "default_entity_id": cfg["entity_id"],
                "state_topic": cfg["state_topic"],
                "json_attributes_template": "{{ value_json | tojson }}",
                "json_attributes_topic": cfg["attributes_topic"],
                "unit_of_measurement": cfg["unit"],
                "device_class": cfg["device_class"],
                "state_class": cfg["state_class"],
                "device": {
                    "identifiers": ["utilityapi_eversource_gateway"],
                    "name": "Eversource Energy Monitor (via pyiu's UtilityAPI Integration)",
                    "manufacturer": "Itron",
                    "model": "G5R1",
                    "sw_version": SCRIPT_VERSION
                }
            }
            self.call_service("mqtt/publish", topic=cfg["discovery_topic"], payload=json.dumps(payload), retain=True)
            self.log(f"Sent MQTT Discovery payload to provision child sensor {cfg['name']}")

    def fetch_utilityapi_data(self, start_date, end_date):
        url = "https://utilityapi.com/api/v2/intervals"
        headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
        params = {"meters": METER_ID, "start": start_date, "end": end_date}

        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()

        intervals = response.json().get("intervals", [])
        # Sample readout of INTERVALS
        # [
        #     {
        #         "uid": "2061339-1779883200-1779508800",
        #         "meter_uid": "2061339",
        #         "authorization_uid": "579849",
        #         "created": "2026-05-24T16:47:41.205121+00:00",
        #         "updated": "2026-05-27T16:48:25.301699+00:00",
        #         "notes": [],
        #         "utility": "EVRSRCMA",
        #         "blocks": [
        #             "base",
        #             "sources",
        #             "readings"
        #         ],
        #         "base": {
        #             "billing_account": "74010801889",
        #             "service_identifier": "740108018890071454546",
        #             "service_tariff": "R1/EMA",
        #             "service_address": "46 SWIFT RD, FRAMINGHAM, MA 01702",
        #             "meter_numbers": [],
        #             "qualities": []
        #         },
        #         "sources": [
        #             {
        #                 "type": "greenbutton_cmd",
        #                 "raw_url": null
        #             }
        #         ],
        #         "readings": [
        #             {
        #                 "start": "2026-05-27T07:45:00.000000-04:00",
        #                 "end": "2026-05-27T08:00:00.000000-04:00",
        #                 "kwh": 0.166,
        #                 "datapoints": [
        #                     {
        #                         "type": "net",
        #                         "unit": "kwh",
        #                         "value": 0.166,
        #                         "meter_number": "0061266661-51718161"
        #                     },
        #                     {
        #                         "type": "max",
        #                         "unit": "kw",
        #                         "value": 0.664,
        #                         "meter_number": "0061266661-51718161"
        #                     },
        #                     {
        #                         "type": "fwd",
        #                         "unit": "kwh",
        #                         "value": 0.166,
        #                         "meter_number": "0061266661-51718161"
        #                     }
        #                 ]
        #             },
        #             {},...
        #         ]
        #     },
        # ]

        readings = [this_interval.get("readings", []) for this_interval in intervals]
        readings_flattened = [item for sublist in readings for item in sublist]

        return readings_flattened

    def connect_websocket(self):
        try:
            ws = websocket.create_connection(HA_URL)
            auth_req = json.loads(ws.recv())
            if auth_req.get("type") == "auth_required":
                ws.send(json.dumps({
                    "type": "auth",
                    "access_token": self.args["token"]
                }))
                if json.loads(ws.recv()).get("type") != "auth_ok":
                    self.error("WS Auth failed.")
                    return None
            return ws
        except Exception as e:
            self.error(f"WS Connection failed: {e}")
            return None

    def get_last_sync_info(self, entity_id):
        """Source of truth: Current sensor state and high-res bookmark."""
        state_info = self.get_state(entity_id, attribute="all")

        if state_info:
            try:
                state_val = state_info.get("state")
                last_sum = float(state_val)
            except Exception as e:
                raise Exception(f"Unable to retrieve state of entity '{entity_id}' due to '{e}'.")

            try:
                last_time_str = state_info.get("attributes", {}).get(LAST_TIME_ATTR_STR)
                last_time_str = datetime.fromisoformat(last_time_str).astimezone(tz=UTC)
            except Exception as e:
                raise Exception(f"Unable to retrieve attribute '{LAST_TIME_ATTR_STR}' of entity '{entity_id}' due to '{e}'.")

        else:
            last_sum = 0.0
            last_time_str = datetime.now(UTC) - timedelta(days=MAX_DAYS)
            self.log(f"Sensor entity '{entity_id}' could not be found or read. Using defaults: {last_sum}, {last_time_str}")
             
        return last_sum, last_time_str

    def insert_statistics(self, ws, message_id, timestamp, cumulative_val, cfg):
        """Inserts LTS record dynamically using targeted metadata configurations."""
        insert_message = json.dumps({
            "id": message_id,
            "type": "recorder/import_statistics",
            "metadata": {
                "has_mean": True if cfg["device_class"] == "power" else False,
                "has_sum": True if cfg["device_class"] == "energy" else False,
                "name": cfg["name"],
                "source": "recorder",
                "statistic_id": cfg["entity_id"],
                "unit_of_measurement": cfg["unit"],
            },
            "stats": [{
                "start": timestamp.isoformat(),
                "state": cumulative_val,
                **({"sum": cumulative_val} if cfg["device_class"] == "energy" else {}),
            }]
        })
        ws.send(insert_message)
        res = json.loads(ws.recv())
        if not res.get("success"):
            self.error(f"LTS Insert error for {cfg['entity_id']} at {timestamp}: {res.get('error')}")

    def fetch_and_import_utilityapi_data(self, kwargs):
        # 1. Fetch UtilityAPI data
        end_date = datetime.now(UTC).date()
        start_date = end_date - timedelta(days=MAX_DAYS)

        try:
            raw_points = self.fetch_utilityapi_data(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
        except Exception as e:
            self.error(f"API Fetch Error: {e}")
            return

        if not raw_points:
            self.log("No new raw intervals from API.")
            return

        # 2. Connect to HA WebSockets
        ws = self.connect_websocket()
        if not ws:
            self.error("WebSocket connection failed. Aborting database synchronization.")
            return

        msg_id = 1

        # 3. Process records per configured sensor mapping
        for sensor_key, cfg in SENSORS_CONFIG.items():
            current_val, last_high_res_time = self.get_last_sync_info(cfg["entity_id"])

            # Filter intervals new to this sensor
            new_points = []
            for p in raw_points:
                ts = datetime.fromisoformat(p.get("start")).astimezone(tz=UTC)
                if ts > last_high_res_time:
                    new_points.append(p)

            if not new_points:
                self.log("No new intervals found. Skipping database synchronization.")
                continue

            new_points.sort(key=lambda x: x.get("start"))

            filtered_points = {}
            for record in new_points:
                for datapoint in record.get("datapoints"):
                    if datapoint.get("type") != cfg["datapoint_type"]:
                        continue
                    else:
                        usage = datapoint.get("value")
                        ts = datetime.fromisoformat(record.get("start")).astimezone(tz=UTC)

                        # Aggregate points into hourly buckets
                        # if "cumulative" in sensor_key.lower(): 
                        #     ts = ts.replace(minute=0, second=0, microsecond=0)
                        ts = ts.replace(minute=0, second=0, microsecond=0) # HA long term statistics rigidly enforces hourly buckets

                        if cfg["device_class"] == "power":
                            filtered_points[ts] = max(filtered_points.get(ts, 0), usage)
                        else:
                            filtered_points[ts] = filtered_points.get(ts, 0.0) + usage

                        # Keep track of the absolute latest interval for the bookmark
                        if ts > last_high_res_time:
                            last_high_res_time = ts

            def my_rounding_func(val):
                return round(val, 3)

            # Publish stats sequentially to HA Database via HA WebSockets
            sorted_ts = sorted(filtered_points.keys())
            if "cumulative" in sensor_key.lower():
                for ts in sorted_ts:
                    current_val += filtered_points[ts]
                    self.insert_statistics(ws, msg_id, ts, my_rounding_func(current_val), cfg)
                    msg_id += 1
            else:
                for ts in sorted_ts:
                    current_val = filtered_points[ts]
                    self.insert_statistics(ws, msg_id, ts, my_rounding_func(current_val), cfg)
                    msg_id +=1 

            # Update Home Assistant Entity Status via MQTT State & Attributes Topics
            attrs_payload = {f"{LAST_TIME_ATTR_STR}": last_high_res_time.isoformat()}
            self.call_service("mqtt/publish", topic=cfg["state_topic"], payload=str(my_rounding_func(current_val)), retain=True)
            self.call_service("mqtt/publish", topic=cfg["attributes_topic"], payload=json.dumps(attrs_payload), retain=True)

            self.log(f"Successfully processed {len(sorted_ts)} hours ({len(new_points)} points retained out of {len(raw_points)}) for sensor {cfg['entity_id']}. Latest value: {my_rounding_func(current_val)} {cfg['unit']}")

        ws.close()
