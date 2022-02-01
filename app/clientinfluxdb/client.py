import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
import click
from datetime import datetime, timedelta
import random

bucket = "telemetry"
org = "circutor"
token = "xIBmBP8B0thtODu4h_ky4t2I3pQpo9LHpEwWY5aqnbVgn-YXuzFkn-uDJpJYJQdlJWc4YlHm_KLO55k0UiaULw=="
# Store the URL of your InfluxDB instance
url="http://localhost:8086"

@click.command()
#@click.option('--url', default="https://powerstudio.circutor.com", help='Power Studio Url')
#@click.option('--bucket', envvar="BUCKET", help='S3 Bucket')
def run():
    print('uploading test data to influx db')
    client = influxdb_client.InfluxDBClient(
       url=url,
       token=token,
       org=org
    )

    write_api = client.write_api(write_options=SYNCHRONOUS)

    now = datetime(2022, 1, 25)
    while now < datetime(2022, 1, 28):
        print(now)
        p = influxdb_client.Point("energy").tag("location", "viladecavalls").tag("device", "raptor21").field("voltage", random.randint(2100, 2300) / 10).field("power", random.randint(300, 350) / 10).time(now)
        write_api.write(bucket=bucket, org=org, record=p)
        now = now + timedelta(minutes=5)


if __name__ == "__main__":
    run()
