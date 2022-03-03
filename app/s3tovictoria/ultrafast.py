import time
import boto3
from io import BytesIO
import json
from datetime import datetime
import requests
import click


def get_bucket_keys(bucket_name, prefix='/', delimiter='/'):
    prefix = prefix[1:] if prefix.startswith(delimiter) else prefix
    bucket = boto3.resource('s3').Bucket(bucket_name)
    return (_.key for _ in bucket.objects.filter(Prefix=prefix))


def to_victoria_import(raw_data):
    tags = {}
    metrics = {}

    for row in raw_data.splitlines():
        data = json.loads(row)

        ts = int(datetime.timestamp(datetime.strptime(data["ts"], "%Y-%m-%dT%H:%M:%S")) * 1000)
        for tag, value in data["tags"].items():
            if tag not in tags:
                tags[tag] = value
        tags['device_id'] = data["device_id"]
        for metric, value in data["values"].items():
            if metric not in metrics:
                metrics[metric] = []

            p = (ts, value)
            metrics[metric].append(p)

    output = ""
    # {"metric":{"__name__":"bid","market":"NASDAQ","ticker":"MSFT"},"values":[1.67],"timestamps":[1583865146520]}
    for metric, points in metrics.items():
        line = {'metric': {'__name__': metric }}
        for tag, value in tags.items():
            line['metric'][tag] = value
        timestamps = []
        values = []
        for (ts, value) in points:
            if type(value) is not str:
                values.append(value)
                timestamps.append(ts)
        # check len
        if len(timestamps) != len(values):
            print('THERE IS A MISMATCH BETWEEN VALUES AND TIMESTAMPS')
        line['values'] = values
        line['timestamps'] = timestamps
        if len(line['values']) > 0:
            output = output + json.dumps(line) + "\n"
    return output


def frombucketnofilter(s3bucket, prefix, victoriam_url):
    s3 = boto3.client('s3')
    files = get_bucket_keys(s3bucket, prefix)
    total = 0
    for item in files:
        filename = item.split('/')[-2] + "_" + item.split('/')[-1]
        print('PROCESSING', item)
        epoch = time.time()
        buf = BytesIO()
        s3.download_fileobj(s3bucket, item, buf)
        download_time = time.time() - epoch
        raw_data = buf.getvalue().decode("utf-8")
        output = to_victoria_import(raw_data)

        total_time = time.time() - epoch
        write_time = total_time - download_time
        r = requests.post(victoriam_url + "/api/v1/import", output)
        print(r)
        print(item, 'total:', total_time, 'download:', download_time, 'write:', write_time, 'status:', r.status_code)



@click.command()
@click.option('--s3bucket', default=None, help='S3 to get data from')
@click.option('--prefix', default=None, help='S3 prefix')
@click.option('--victoriam_url', default="http://localhost:8428")
def run(s3bucket, prefix, victoriam_url):
    print(s3bucket, prefix, victoriam_url)
    if prefix and s3bucket and victoriam_url:
        print('loading data from s3')
        frombucketnofilter(s3bucket, prefix, victoriam_url)

if __name__ == "__main__":
    run()

