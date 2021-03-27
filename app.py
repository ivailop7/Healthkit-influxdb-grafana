import json
import sys
import socket
import logging
from datetime import datetime
from flask import request, Flask
from influxdb import InfluxDBClient
from geolib import geohash

DATAPOINTS_CHUNK = 80000

logger = logging.getLogger("console-output")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

app = Flask(__name__)
app.debug = True

client = InfluxDBClient(host='localhost', port=8086)
client.create_database('db')
client.switch_database('db')

@app.route('/collect', methods=['POST', 'GET'])
def collect():
    logger.info(f"Request received")
    
    healthkit_data = None
    transformed_data = []

    try:
        healthkit_data = json.loads(request.data)
    except:
        return "Invalid JSON Received", 400
    
    try:
        logger.info(f"Ingesting Metrics")
        for metric in healthkit_data.get("data", {}).get("metrics", []):
            number_fields = []
            string_fields = []
            for datapoint in metric["data"]:
                metric_fields = set(datapoint.keys())
                metric_fields.remove("date")
                for mfield in metric_fields:
                    if type(datapoint[mfield]) == int or type(datapoint[mfield]) == float:
                        number_fields.append(mfield)
                    else:
                        string_fields.append(mfield)
                point = {
                    "measurement": metric["name"],
                    "time": datapoint["date"],
                    "tags": {str(nfield): str(datapoint[nfield]) for nfield in string_fields},
                    "fields": {str(nfield): float(datapoint[nfield]) for nfield in number_fields}
                }
                transformed_data.append(point)
                number_fields.clear()
                string_fields.clear()

        logger.info(f"Data Transformation Complete")
        logger.info(f"Number of data points to write: {len(transformed_data)}")
        logger.info(f"DB Write Started")

        for i in range(0, len(transformed_data), DATAPOINTS_CHUNK):
            logger.info(f"DB Writing chunk")
            client.write_points(transformed_data[i:i + DATAPOINTS_CHUNK])
        
        logger.info(f"DB Metrics Write Complete")
        logger.info(f"Ingesting Workouts Routes")

        transformed_workout_data = []
        
        for workout in healthkit_data.get("data", {}).get("workouts", []):
            tags = {
                "id": workout["name"] + "-" + workout["start"] + "-" + workout["end"]
            }
            for gps_point in workout["route"]:
                point = {
                    "measurement": "workouts",
                    "time": gps_point["timestamp"],
                    "tags": tags,
                    "fields": {
                        "lat": gps_point["lat"],
                        "lng": gps_point["lon"],
                        "geohash": geohash.encode(gps_point["lat"], gps_point["lon"], 7),
                    }
                }
                transformed_workout_data.append(point)

            for i in range(0, len(transformed_workout_data), DATAPOINTS_CHUNK):
                logger.info(f"DB Writing chunk")
                client.write_points(transformed_workout_data[i:i + DATAPOINTS_CHUNK])
        
        logger.info(f"Ingesting Workouts Complete")
    except:
        logger.exception("Caught Exception. See stacktrace for details.")
        return "Server Error", 500

    return "Success", 200

if __name__ == "__main__":
    hostname = socket.gethostname()
    ip_address = socket.gethostbyname(hostname)
    logger.info(f"Local Network Endpoint: http://{ip_address}/collect")
    app.run(host='0.0.0.0', port=5353)
