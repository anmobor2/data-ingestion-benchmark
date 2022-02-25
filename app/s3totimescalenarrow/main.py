import paho.mqtt.client as mqtt
import click
from app.s3totimescalenarrow.timescalenarrowtable import run

if __name__ == "__main__":
    run()
