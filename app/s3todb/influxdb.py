import click
import boto3
import json
from io import StringIO, BytesIO
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS, WriteOptions, ASYNCHRONOUS
filter = ["AE", "AI1", "AI2", "AI3", "API1", "API2", "API3", "API", "CE", "IE", "IPI1", "IPI2", "IPI3", "PFI1", "PFI2", "PFI3", "VAI", "VI1", "VI2", "VI3"]

#s3_paginator = boto3.client('s3').get_paginator('list_objects_v2')


idb_bucket = "energydata"
org = "circutor"
token = "k3TMVP_NnTnMH-NTEwNehH8CzYkzPoshZNUqPQ1SLM8PxA8e-SePmItW-d_wIadXtCNNgMG-weAWOIYlBZNUzw=="
# Store the URL of your InfluxDB instance
url="http://localhost:8086"


def keys(bucket_name, prefix='/', delimiter='/'):
    prefix = prefix[1:] if prefix.startswith(delimiter) else prefix
    bucket = boto3.resource('s3').Bucket(bucket_name)
    return (_.key for _ in bucket.objects.filter(Prefix=prefix))

@click.command()
@click.option('--s3bucket', help='S3 to get data from')
@click.option('--prefix', default="", help='S3 prefix')
def run(s3bucket, prefix):
    s3 = boto3.client('s3')

    client = influxdb_client.InfluxDBClient(
       url=url,
       token=token,
       org=org
    )

    write_api = client.write_api(write_options=ASYNCHRONOUS)


    files = keys(s3bucket, prefix)
    total = 0
    for item in files:
        print(item)
        buf = BytesIO()
        s3.download_fileobj(s3bucket, item, buf)
        json_object = json.loads(buf.getvalue().decode("utf-8"))

        p = influxdb_client.Point("energy")
        for tag, value in json_object['tags'].items():
            p.tag(tag, value)

        for var, value in json_object['values'].items():
            p.field(var, value)
        p.time(json_object['ts'])
        write_api.write(bucket=idb_bucket, org=org, record=p)


        total = total + 1
    print(total)


if __name__ == "__main__":
    run()
