import click
import boto3
import json
from io import StringIO, BytesIO
from app.lib import remotewrite
from datetime import datetime
filter = ["AE", "AI1", "AI2", "AI3", "API1", "API2", "API3", "API", "CE", "IE", "IPI1", "IPI2", "IPI3", "PFI1", "PFI2", "PFI3", "VAI", "VI1", "VI2", "VI3"]

def keys(bucket_name, prefix='/', delimiter='/'):
    prefix = prefix[1:] if prefix.startswith(delimiter) else prefix
    bucket = boto3.resource('s3').Bucket(bucket_name)
    return (_.key for _ in bucket.objects.filter(Prefix=prefix))

@click.command()
@click.option('--s3bucket', help='S3 to get data from')
@click.option('--prefix', default="", help='S3 prefix')
def run(s3bucket, prefix):
    s3 = boto3.client('s3')

    files = keys(s3bucket, prefix)
    total = 0
    for item in files:
        print(item)
        buf = BytesIO()
        s3.download_fileobj(s3bucket, item, buf)
        json_object = json.loads(buf.getvalue().decode("utf-8"))
        ts = datetime.strptime(json_object["ts"], "%Y-%m-%dT%H%M%S")
        for var, value in json_object['values'].items():
            if var in filter:
                remotewrite.write("http://localhost:9201/write", ts, value, var, json_object["tags"])

        total = total + 1
    print(total)


if __name__ == "__main__":
    run()
