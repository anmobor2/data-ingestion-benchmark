import psycopg2
from  psycopg2.extras import DictCursor
from sshtunnel import SSHTunnelForwarder
import click
from app.lib import remotewrite
from datetime import datetime, timedelta
import time
from urllib.parse import urlparse
import io
import boto3
import json
import os
from pathlib import Path

# https://gist.github.com/deehzee/53c8708417312e5deb32c58b73dca7a5

variables = [
    "OPERATING_HOURS", "PH_CON_TOT", "PH_GEN_TOT", "QH_Q1_TOT", "QH_Q2_TOT", "QH_Q3_TOT", "QH_Q4_TOT", "REPAIR_HOURS",
    "V_PP", "THDV_PP", "THDI"
    "REPCONN_STEP01", "REPCONN_STEP02", "REPCONN_STEP03", "REPCONN_STEP04", "REPCONN_STEP05", "REPCONN_STEP06", "REPCONN_STEP07", "REPCONN_STEP08", "REPCONN_STEP09", "REPCONN_STEP10", "REPCONN_STEP11", "REPCONN_STEP12", "POWLOSS_STEP01", "POWLOSS_STEP02", "POWLOSS_STEP03", "POWLOSS_STEP04", "POWLOSS_STEP05",
    "POWLOSS_STEP06", "POWLOSS_STEP07", "POWLOSS_STEP08", "POWLOSS_STEP09", "POWLOSS_STEP10", "POWLOSS_STEP11", "POWLOSS_STEP12",
    "OP_COUNT_STEP1", "OP_COUNT_STEP2", "OP_COUNT_STEP3", "OP_COUNT_STEP4", "OP_COUNT_STEP5", "OP_COUNT_STEP6", "OP_COUNT_STEP7", "OP_COUNT_STEP8", "OP_COUNT_STEP9", "OP_COUNT_STEP10", "OP_COUNT_STEP11", "OP_COUNT_STEP12",
    "cosPhiDaily", "cosPhiWeekly"
]

LABELS_CACHE = {}

s3 = boto3.client('s3')


def get_labels(cur, device_id):
    labels = {}
    sql = "SELECT * FROM public.attribute_kv where entity_id = '{device_id}' order by attribute_key;".format(device_id=device_id)
    cur.execute(sql)
    for label in cur.fetchall():
        if label['str_v']:
            labels[label['attribute_key']] = label['str_v']
    return labels


def run_sql(cur, sql):
    r = cur.execute(sql)
    return r


def write_json(data, prometheus_url):
    for var, value in data['values'].items():
        remotewrite.write(prometheus_url, data["ts"], value, var, data["tags"])

def build_sql(day, variables):
    sql = """select 
                   to_timestamp(values.ts / 1000) as ts, values.entity_id as device_id, \n"""
    for var in variables:
        sql = sql + "MAX(CASE WHEN vars.key = '{var}' THEN values.{field} END) AS {var}, \n".format(var=var,
                                                                                                    field='long_v')
    sql = sql + """count(*) as num_vars 
                   FROM public.ts_kv as values 
                   LEFT JOIN public.ts_kv_dictionary as vars on values.key = vars.key_id 
                   WHERE to_timestamp(values.ts / 1000)::date = '{day}'
                   GROUP BY (values.ts, values.entity_id)
                   """.format(day=day)
    return sql

def row_to_json(row, cur):
    if row['device_id'] not in LABELS_CACHE:
        LABELS_CACHE[row['device_id']] = get_labels(cur, row['device_id'])
    values = {k: v for k, v in row.items() if v and k not in ["ts", "device_id", "num_vars"] is not None}
    data = {"ts": row["ts"], "device_id": row["device_id"], 'values': values, 'tags': LABELS_CACHE[row['device_id']]}
    return data

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime)):
        return obj.strftime("%Y-%m-%dT%H:%M:%S")
    raise TypeError ("Type %s not serializable" % type(obj))

def save_s3(data, bucket):
    buff = io.StringIO()
    buff.write(json.dumps(data, default=json_serial))
    filename = "tbdata/{device}/{ts}.json".format(device=data["device_id"], ts=data["ts"].strftime("%Y-%m-%dT%H:%M:%S"))
    s3.upload_fileobj(
        io.BytesIO(buff.getvalue().encode()),
        bucket,
        filename
    )
    print('STORED FILE as ', filename, 'IN', bucket)

def save_local(data, path):
    filename = "{path}/{device}/{ts}.json".format(path=path, device=data["device_id"], ts=data["ts"].strftime("%Y-%m-%dT%H:%M:%S"))
    Path("{path}/{device}".format(path=path, device=data["device_id"])).mkdir(parents=True, exist_ok=True)
    with open(filename, 'w') as f:
        f.write(json.dumps(data, default=json_serial))
    print('STORED FILE as ', filename)

@click.command()
@click.option('--prometheus-url', default=None, help='Prometheus url')
@click.option('--start', default="", help='start day')
@click.option('--end', default="", help='end day (not included)')
@click.option('--postgres', default="", help='Postgres URI')
@click.option('--bucket', default=None, help='S3 Bucket')
@click.option('--path', default=None, help='Path to store data locally')
@click.option('--bastion-host', default="", help='Bastion Host')
@click.option('--bastion-user', default="", help='Bastion User')
@click.option('--bastion-key', default="", help='Bastion Key')
def run(prometheus_url, start, end, postgres, bucket, bastion_host, bastion_user, bastion_key, path):
    local_port = 6543
    uri = urlparse(postgres)

    # Alternatively use contexts...
    with SSHTunnelForwarder(
            (bastion_host, 22),
            ssh_username=bastion_user,
            ssh_private_key=bastion_key,
            remote_bind_address=(uri.hostname, uri.port) #,
            #local_bind_address=('localhost', local_port),  # could be any available port
    ) as tunnel:
        local_uri = postgres.replace(uri.hostname, tunnel.local_bind_host).replace(str(uri.port), str(tunnel.local_bind_port))
        print(local_uri)
        with psycopg2.connect(local_uri) as connect:
            cur = connect.cursor(cursor_factory=DictCursor)
            day = datetime.strptime(start, "%Y-%m-%d")

            while day < datetime.strptime(end, "%Y-%m-%d"):
                epoch = time.time()
                print('Getting', day)
                sql = build_sql(day.strftime("%Y-%m-%d"), variables)
                #print(sql)
                cur.execute(sql)
                num_rows = cur.rowcount
                for row in cur.fetchall():
                    data = row_to_json(row, cur)
                    #print(data)
                    if len(data["values"]) > 0:
                        if bucket:
                            save_s3(data, bucket)
                        if path:
                            save_local(data, path)
                        if prometheus_url:
                            write_json(data, prometheus_url)

                print('Total rows', num_rows, 'in', time.time() - epoch, 's')
                day = day + timedelta(days=1)

if __name__ == "__main__":
    run()
