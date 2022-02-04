import click
import boto3
import json
from io import StringIO, BytesIO

s3_paginator = boto3.client('s3').get_paginator('list_objects_v2')


def keys(bucket_name, prefix='/', delimiter='/'):
    prefix = prefix[1:] if prefix.startswith(delimiter) else prefix
    bucket = boto3.resource('s3').Bucket(bucket_name)
    return (_.key for _ in bucket.objects.filter(Prefix=prefix))

@click.command()
@click.option('--bucket', help='S3 to get data from')
@click.option('--prefix', default="", help='S3 prefix')
def run(bucket, prefix):
    s3 = boto3.client('s3')

    files = keys(bucket, prefix)
    total = 0
    for item in files:
        print(item)
        buf = BytesIO()
        s3.download_fileobj(bucket, item, buf)
        json_object = json.loads(buf.getvalue().decode("utf-8"))

        total = total + 1
    print(total)


if __name__ == "__main__":
    run()
