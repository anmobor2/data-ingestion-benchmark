from app.lib import PowerStudio
import boto3
import io
import json
from datetime import datetime
import time
import sys
import click
import os
import signal

class GracefulKiller:
  kill_now = False
  def __init__(self):
    signal.signal(signal.SIGINT, self.exit_gracefully)
    signal.signal(signal.SIGTERM, self.exit_gracefully)

  def exit_gracefully(self, *args):
    self.kill_now = True

'''
devices = ["0CR02-0", "0CR04-1.AC-Meter", "0CR02-12.PLUG A - Meter", "4CR02-13.PLUG A - Meter",
               "0CR04-3.PLUG A - Meter", "4CR02-13.EVSE.PLUG A"]
'''

BUCKET = "myc-ingestion-sample-recorded-data"

s3 = boto3.client('s3')

def get_data(url, devices, ts):
    ps = PowerStudio(url)
    for device in devices.split(','):
        buff = io.StringIO()
        try:
            row = ps.get_json(device, ts=ts)
        except Exception as e:
            print('ERROR GETTING', device, '=>', e)
        print(row)
        buff.write(json.dumps(row))
        s3.upload_fileobj(
            io.BytesIO(buff.getvalue().encode()),
            BUCKET,
            "psdata/{device}/".format(device=device) + ts.strftime("%Y%m%dT%H%M%S") + ".json"
        )


@click.command()
@click.option('--url', default="https://powerstudio.circutor.com", help='Power Studio Url')
@click.option('--devices', envvar="DEVICES", help='List of devices')
@click.option('--interval', default=5, help='Interval between records')
def run(url, devices, interval):
    killer = GracefulKiller()
    print('GETTING', devices, 'FROM', url, 'AT INTERVAL', interval)
    while not killer.kill_now:
        now = datetime.now()
        print(now)
        if now.second == 0 and now.minute % interval == 0:
            get_data(url, devices, now)
        time.sleep(1)
        sys.stdout.flush()
    print("End of the program. I was killed gracefully :)")

if __name__ == "__main__":
    run()
