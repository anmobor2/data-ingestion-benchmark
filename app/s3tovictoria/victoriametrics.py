# working victoria, influxdb not working
import click
import boto3
import json
import time
from io import StringIO, BytesIO
from datetime import datetime, timezone

from app.s3tovictoria import remotewrite

def frombucket(s3bucket, prefix, victoriam_url, devicesfile):
    s3 = boto3.client('s3')
    files = keys(s3bucket, prefix)
    total = 0
    for item in files:
        epoch = time.time()
        buf = BytesIO()
        s3.download_fileobj(s3bucket, item, buf)
        json_object = json.loads(buf.getvalue().decode("utf-8"))
        download_time = time.time() - epoch
        #        ts = datetime.strptime(json_object["ts"], "%Y-%m-%dT%H%M%S")
        dtime = str(datetime.now(timezone.utc))
        dt = dtime[0:10] + "T" + dtime[11:19]
        ts = datetime.strptime(dt, '%Y-%m-%dT%H:%M:%S')
        device_id = json_object["device_id"]
        if devicesfile:
            with open(devicesfile, 'r') as filedevices:
                for line in filedevices:
                    device = line.replace('\n', '')
                    print(device.rstrip())
                    if device == device_id:
                        print("Device == Device")
                        for var, value in json_object['values'].items():
                            if var in filter:
                                remotewrite.write(victoriam_url, ts, value, var, json_object["tags"])
                        total_time = time.time() - epoch
                        write_time = total_time - download_time

                        total = total + 1
                        print(item, 'total:', total_time, 'download:', download_time, 'write:', write_time)

    if total == 0:
        print("DEVICE ID NOT FOUND IN DEVICES FILE 0 PROCESSED")


def frombucketnofilter(s3bucket, prefix, victoriam_url):
    s3 = boto3.client('s3')
    files = keys(s3bucket, prefix)
    total = 0
    for item in files:
        epoch = time.time()
        buf = BytesIO()
        s3.download_fileobj(s3bucket, item, buf)
        download_time = time.time() - epoch
        data = buf.getvalue().decode("utf-8").splitlines()

        for row in data:
            json_object = json.loads(row)
            ts = datetime.strptime(json_object["ts"], "%Y-%m-%dT%H:%M:%S")
            for var, value in json_object['values'].items():
                remotewrite.write(victoriam_url, ts, value, var, json_object["tags"])

            total = total + 1
        total_time = time.time() - epoch
        write_time = total_time - download_time

        print(item, 'total:', total_time, 'download:', download_time, 'write:', write_time)


def fromfile(filename, devicesfile, victoriam_url):
    if filename:
        with open(filename, 'r') as f:
            if filename:
                with open(filename, 'r') as f:
                    json_object = json.loads(f.read())
                    device_id = json_object["device_id"]
                    tags = json_object["tags"]
                    dtime = str(datetime.now(timezone.utc))
                    dt = dtime[0:10] + "T" + dtime[11:19]
                    ts = datetime.strptime(dt, '%Y-%m-%dT%H:%M:%S')
                    if devicesfile:
                        with open(devicesfile, 'r') as filedevices:
                            for line in filedevices:
                                device = line.replace('\n', '')
                                if device == device_id:
                                    for var, value in json_object['values'].items():
                                        remotewrite.write(victoriam_url, ts, value, var, json_object["tags"])
                                    break

def keys(bucket_name, prefix='/', delimiter='/'):
    prefix = prefix[1:] if prefix.startswith(delimiter) else prefix
    bucket = boto3.resource('s3').Bucket(bucket_name)
    return (_.key for _ in bucket.objects.filter(Prefix=prefix))

@click.command()
@click.option('--s3bucket', default=None, help='S3 to get data from')
@click.option('--prefix', default=None, help='S3 prefix')
@click.option('--filename', default=None)
@click.option('--devicesfile', default=None)
@click.option('--victoriam_url', default="http://localhost:8428/api/v1/write")
def run(s3bucket, prefix, victoriam_url, filename, devicesfile):

    if prefix and s3bucket and victoriam_url:
        #frombucket(s3bucket, prefix, victoriam_url, devicesfile)
        print('loading data from s3')
        frombucketnofilter(s3bucket, prefix, victoriam_url)

    if filename and devicesfile and victoriam_url:
        fromfile(filename, devicesfile, victoriam_url)





if __name__ == "__main__":
    run()
