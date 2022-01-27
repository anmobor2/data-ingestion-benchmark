from app.lib.powerstudio import PowerStudio
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


s3 = boto3.client('s3')

def get_data(url, devices, ts, bucket):
    ps = PowerStudio(url)
    for device in devices.split(','):
        buff = io.StringIO()
        try:
            row = ps.get_json(device, ts=ts)
        except Exception as e:
            print('ERROR getting', device, '=>', e)
        print('got', len(row['values']), 'variables from', device)
        buff.write(json.dumps(row))
        filename = "psdata/{device}/{ts}.json".format(device=device, ts=ts.strftime("%Y%m%dT%H%M%S"))
        s3.upload_fileobj(
            io.BytesIO(buff.getvalue().encode()),
            bucket,
            filename
        )
        print('STORED FILE as ', filename, 'IN', bucket)


@click.command()
@click.option('--url', default="https://powerstudio.circutor.com", help='Power Studio Url')
@click.option('--devices', envvar="DEVICES", help='List of devices')
@click.option('--bucket', envvar="BUCKET", help='S3 Bucket')
@click.option('--interval', default=5, help='Interval between records')
@click.option('--daemon/--no-daemon', default=True)
def run(url, devices, interval, daemon, bucket):
    if not daemon:
        print('getting', devices, 'from', url, 'a single time', interval)
        now = datetime.now()
        get_data(url, devices, now, bucket)
        exit()

    print('getting', devices, 'from', url, 'at interval', interval)
    killer = GracefulKiller()
    while not killer.kill_now:
        now = datetime.now()
        if now.second == 0:
            print(now)
        if now.second == 0 and now.minute % interval == 0:
            get_data(url, devices, now, bucket)
        time.sleep(1)
        sys.stdout.flush()
    print("End of the program. I was killed gracefully :)")

if __name__ == "__main__":
    run()
