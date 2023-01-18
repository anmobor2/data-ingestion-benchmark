# one column for each field, one row for all 
import click
import json
import psycopg2
from psycopg2.extras import RealDictCursor
import boto3
from io import BytesIO
import time

def keys(bucket_name, prefix='/', delimiter='/'):
    prefix = prefix[1:] if prefix.startswith(delimiter) else prefix
    bucket = boto3.resource('s3').Bucket(bucket_name)
    return (_.key for _ in bucket.objects.filter(Prefix=prefix))


def data_to_sql(raw_data):
    data = json.loads(raw_data)

    fields = ["device_id", "time"]
    values = ["'" + data["device_id"] + "'", "'" + data["ts"] + "'"]

    for key, value in data["values"].items():
        if type(value) is not str:
            fields.append(key)
            values.append(str(value))

    sql = "INSERT INTO metric_data ({fields}) VALUES({values})".format(
        fields=",".join(fields),
        values=",".join(values)
    )

    return data["device_id"], data["tags"], sql



def insert_file(cursor, obj):
    device_id, tags, data_sql = data_to_sql(obj)

    # check if device exists
    cursor.execute("SELECT count(*) as num FROM devices where id = '{device_id}'".format(device_id=device_id))
    r = cursor.fetchone()
    if r["num"] == 0:
        sql_device = "INSERT INTO devices (id, tags) VALUES('{id}', '{tags}')".format(id=device_id,
                                                                                      tags=json.dumps(tags))
        r = cursor.execute(sql_device)

    #print(data_sql)
    r = cursor.execute(data_sql)




@click.command()
@click.option('--s3bucket', default=None, help='S3 to get data from')
@click.option('--prefix', default=None, help='S3 prefix')
@click.option('--uri', default=None, help='Timescale URI')
@click.option('--filename', default=None)
#@click.option('--devicesfile', default=None)
def run(s3bucket, prefix, filename, uri):
    db_conn = psycopg2.connect(uri)
    cursor = db_conn.cursor(cursor_factory=RealDictCursor)
    s3 = boto3.client('s3')


    #if devicesfile:
    #    with open(devicesfile, 'r') as filedevices:

    if prefix and s3bucket:
        print("INSIDE BUCKET S3")
        print(prefix, s3bucket)
        files = keys(s3bucket, prefix)
        total = 0
        print(files)
        for item in files:
            epoch = time.time()
            print(item)
            buf = BytesIO()

            s3.download_fileobj(s3bucket, item, buf)
            data = buf.getvalue().decode("utf-8").splitlines()
            download_time = time.time() - epoch
            for row in data:
                insert_file(cursor, row)
            r = db_conn.commit()
            write_time = time.time() - epoch - download_time
            print('COMMITED FILE', item, r, 'download time:', download_time, 'write time:', write_time)


if __name__ == "__main__":
    run()
