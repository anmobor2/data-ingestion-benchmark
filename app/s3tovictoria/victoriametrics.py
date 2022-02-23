import click
import boto3
import json
import time
from io import StringIO, BytesIO
import remotewrite
from datetime import datetime


def keys(bucket_name, prefix='/', delimiter='/'):
    prefix = prefix[1:] if prefix.startswith(delimiter) else prefix
    bucket = boto3.resource('s3').Bucket(bucket_name)
    return (_.key for _ in bucket.objects.filter(Prefix=prefix))

@click.command()
@click.option('--s3bucket', default="myc-ingestion-sample-recorded-data", help='S3 to get data from')
@click.option('--prefix', default="/tbdata/000e00b0-82b2-11ec-9949-7f0fdad2c99c/", help='S3 prefix')
@click.option('--victoriam_url', default="http://localhost:8428/write")
def run(s3bucket, prefix, victoriam_url):
    s3 = boto3.client('s3')

    files = keys(s3bucket, prefix)
    total = 0
    for item in files:
        epoch = time.time()
        buf = BytesIO()
        s3.download_fileobj(s3bucket, item, buf)
        json_object = json.loads(buf.getvalue().decode("utf-8"))
        download_time = time.time() - epoch
        ts = datetime.strptime(json_object["ts"], "%Y-%m-%dT%H%M%S")
        for var, value in json_object['values'].items():
            if var in filter:
                remotewrite.write(victoriam_url, ts, value, var, json_object["tags"])
        total_time = time.time() - epoch
        write_time = total_time - download_time

        total = total + 1
        print(item, 'total:', total_time, 'download:', download_time, 'write:', write_time)

    print(total)


if __name__ == "__main__":
    run()
