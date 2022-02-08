import psycopg2
from  psycopg2.extras import DictCursor
from sshtunnel import SSHTunnelForwarder
import click

# https://gist.github.com/deehzee/53c8708417312e5deb32c58b73dca7a5

variables = [
    "OPERATING_HOURS", "PH_CON_TOT", "PH_GEN_TOT", "QH_Q1_TOT", "QH_Q2_TOT", "QH_Q3_TOT", "QH_Q4_TOT", "REPAIR_HOURS",
    "V_PP", "THDV_PP", "THDI"
    "REPCONN_STEP01", "REPCONN_STEP02", "REPCONN_STEP03", "REPCONN_STEP04", "REPCONN_STEP05", "REPCONN_STEP06", "REPCONN_STEP07", "REPCONN_STEP08", "REPCONN_STEP09", "REPCONN_STEP10", "REPCONN_STEP11", "REPCONN_STEP12", "POWLOSS_STEP01", "POWLOSS_STEP02", "POWLOSS_STEP03", "POWLOSS_STEP04", "POWLOSS_STEP05",
    "POWLOSS_STEP06", "POWLOSS_STEP07", "POWLOSS_STEP08", "POWLOSS_STEP09", "POWLOSS_STEP10", "POWLOSS_STEP11", "POWLOSS_STEP12",
    "OP_COUNT_STEP1", "OP_COUNT_STEP2", "OP_COUNT_STEP3", "OP_COUNT_STEP4", "OP_COUNT_STEP5", "OP_COUNT_STEP6", "OP_COUNT_STEP7", "OP_COUNT_STEP8", "OP_COUNT_STEP9", "OP_COUNT_STEP10", "OP_COUNT_STEP11", "OP_COUNT_STEP12",
    "cosPhiDaily", "cosPhiWeekly"
]

labels_cache = {}

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


@click.command()
def run():
    # Alternatively use contexts...
    with SSHTunnelForwarder(
            ('54.171.211.53', 22),
            ssh_username='ec2-user',
            ssh_private_key='~/cluster-bastion.pem',
            remote_bind_address=('tsdb-27c921-circutor-f875.a.timescaledb.io', 19637),
            local_bind_address=('localhost', 6543),  # could be any available port
    ) as tunnel:
        with psycopg2.connect(
                user='tsdbadmin',
                password='ft12bvwiplkkt45u',
                database='production',
                host=tunnel.local_bind_host,
                port=tunnel.local_bind_port
        ) as connect:
            cur = connect.cursor(cursor_factory=DictCursor)

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
                """.format(day='2022-02-05')

            print(sql)
            cur.execute(sql)
            num_rows = cur.rowcount
            for row in cur.fetchall():
                print(row)
                if row['device_id'] not in labels_cache:
                    labels_cache[row['device_id']] = get_labels(cur, row['device_id'])
                values = {k: v for k, v in row.items() if v and k not in ["ts", "device_id", "num_vars"] is not None}
                data = {"ts": row["ts"], "device_id": row["device_id"], 'values': values,  'tags': labels_cache[row['device_id']]}
                print(data)
            print('Total rows', num_rows)

if __name__ == "__main__":
    run()
