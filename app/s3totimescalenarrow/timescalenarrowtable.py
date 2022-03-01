import click
import json
import psycopg2
from psycopg2.extras import RealDictCursor
import boto3
from io import BytesIO


# uri="postgres://postgres:admin@127.0.0.1:5432/postgres"
def keys(bucket_name, prefix='/', delimiter='/'):
    prefix = prefix[1:] if prefix.startswith(delimiter) else prefix
    bucket = boto3.resource('s3').Bucket(bucket_name)
    return (_.key for _ in bucket.objects.filter(Prefix=prefix))


def insert_file(cursor, obj, filedevices=None):
    data = json.loads(obj)
    device_id = data["device_id"]
    tags = data["tags"]

    if filedevices:
        for line in filedevices:
            device = line.replace('\n', '')
            print(device.rstrip())
            if device == device_id:
                cursor.execute("SELECT count(*) as num FROM devices where id = '{device_id}'".format(device_id=device_id))
                r = cursor.fetchone()
                if r["num"] == 0:
                    sql_device = "INSERT INTO devices (id, tags) VALUES('{id}', '{tags}')".format(id=device_id,
                                                                                                  tags=json.dumps(tags))
                    cursor.execute(sql_device)

                for key, value in data["values"].items():
                    fields = ["device_id", "time", "metric", "value"]
                    values = ["'" + data["device_id"] + "'", "'" + data["ts"] + "'", "'" + str(key) + "'", str(value)]
                    print(key, value)
                    sql = "INSERT INTO metric_data_narrow ({fields}) VALUES({values})".format(
                        fields=",".join(fields),
                        values=",".join(values)
                    )
                    cursor.execute(sql)


@click.command()
@click.option('--s3bucket', default=None, help='S3 to get data from')
@click.option('--prefix', default=None, help='S3 prefix')
@click.option('--uri', default=None, help='Timescale URI')
@click.option('--filename', default=None)
@click.option('--devicesfile', default=None)
def run(s3bucket, prefix, filename, uri, devicesfile):
    db_conn = psycopg2.connect(uri)
    cursor = db_conn.cursor(cursor_factory=RealDictCursor)
    s3 = boto3.client('s3')

    if devicesfile:
        with open(devicesfile, 'r') as filedevices:
            if filename:
                with open(filename, 'r') as f:
                    insert_file(cursor, f.read(), filedevices)
                    db_conn.commit()

            if prefix and s3bucket:
                print("INSIDE BUCKET S3")
                print(prefix, s3bucket)
                files = keys(s3bucket, prefix)
                total = 0
                print(files)
                for item in files:
                    print(item)
                    buf = BytesIO()

                    s3.download_fileobj(s3bucket, item, buf)
                    insert_file(cursor, buf.getvalue().decode("utf-8"), filedevices)
                    db_conn.commit()


if __name__ == "__main__":
    run()
