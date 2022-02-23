import click
import boto3
from io import StringIO, BytesIO
import json
import base64
import psycopg2
from psycopg2.extras import RealDictCursor

uri = "postgres://postgres:admin@localhost:5432/postgres"
db_conn = psycopg2.connect(uri)
c = db_conn.cursor(cursor_factory=RealDictCursor)
#c.execute("SELECT 1 = 1")
#result = c.fetchone()

def keys(bucket_name, prefix='/', delimiter='/'):
    prefix = prefix[1:] if prefix.startswith(delimiter) else prefix
    bucket = boto3.resource('s3').Bucket(bucket_name)
    return (_.key for _ in bucket.objects.filter(Prefix=prefix))

def query(jsonStr):
    device_id = ""
    if ('device_id' in jsonStr and not jsonStr['device_id'].isspace()):
        device_id = jsonStr['device_id']
    elif ('DEVICE_ID' in jsonStr and not jsonStr['DEVICE_ID'].isspace()):
        device_id = jsonStr['DEVICE_ID']

    tags = jsonStr["tags"].items()
    values = jsonStr["values"]

    OPERATING_HOURS = ""
    if ('operating_hours' in values and not str(values['operating_hours']).isspace()):
        OPERATING_HOURS = values['operating_hours']
    elif ('OPERATING_HOURS' in values and not str(values['OPERATING_HOURS']).isspace()):
        OPERATING_HOURS = values['OPERATING_HOURS']
    PH_CON_TOT = ""
    if ('ph_con_tot' in values and not str(values['ph_con_tot']).isspace()):
        PH_CON_TOT = values['ph_con_tot']
    elif ('PH_CON_TOT' in values and not str(values['PH_CON_TOT']).isspace()):
        PH_CON_TOT = values['PH_CON_TOT']
    PH_GEN_TOT = ""
    if ('ph_gen_tot'  and not str(values['ph_gen_tot']).isspace()):
        PH_GEN_TOT = values['ph_con_tot']
    elif ('PH_GEN_TOT' in values and not str(values['PH_GEN_TOT']).isspace()):
        PH_GEN_TOT = values['PH_GEN_TOT']
    QH_Q1_TOT = ""
    if ('qh_q1_tot' in values and not str(values['qh_q1_tot']).isspace()):
        QH_Q1_TOT = values['qh_q1_tot']
    elif ('QH_Q1_TOT' in values and not str(values['QH_Q1_TOT']).isspace()):
        QH_Q1_TOT = values['QH_Q1_TOT']
    QH_Q2_TOT = ""
    if ('qh_q2_tot' in values and not str(values['qh_q2_tot']).isspace()):
        QH_Q2_TOT = values['ph_con_tot']
    elif ('QH_Q2_TOT' in values and not str(values['QH_Q2_TOT']).isspace()):
        QH_Q2_TOT = values['QH_Q2_TOT']
    QH_Q3_TOT = ""
    if ('qh_q3_tot' in values and not str(values['qh_q3_tot']).isspace()):
        QH_Q3_TOT = values['qh_q3_tot']
    elif ('QH_Q3_TOT' in values and not str(values['QH_Q3_TOT']).isspace()):
        QH_Q3_TOT = values['QH_Q3_TOT']
    QH_Q4_TOT = ""
    if ('qh_q4_tot' in values and not str(values['qh_q4_tot']).isspace()):
        QH_Q4_TOT = values['qh_q4_tot']
    elif ('QH_Q4_TOT' in values and not str(values['QH_Q4_TOT']).isspace()):
        QH_Q4_TOT = values['QH_Q4_TOT']
    REPAIR_HOURS = ""
    if ('repair_hours' in values and not str(values['repair_hours']).isspace()):
        REPAIR_HOURS = values['repair_hours']
    elif ('REPAIR_HOURS' in values and not str(values['REPAIR_HOURS']).isspace()):
        REPAIR_HOURS = values['REPAIR_HOURS']
    V_PP = ""
    if ('v_pp' in values and not str(values['v_pp']).isspace()):
        REPAIR_HOURS = values['v_pp']
    elif ('V_PP' in values and not str(values['V_PP']).isspace()):
        V_PP = values['V_PP']
    THDV_PP = ""
    if ('thdv_pp' in values and not str(values['thdv_pp']).isspace()):
        THDV_PP = values['thdv_pp']
    elif ('THDV_PP' in values and not str(values['THDV_PP']).isspace()):
        THDV_PP = values['THDV_PP']
    THDI = ""
    if ('thdi' in values and not str(values['thdi']).isspace()):
        THDI = values['thdi']
    elif ('THDI' in values and not str(values['THDI']).isspace()):
        THDI = values['THDI']

    REPCONN_STEP01 = ""
    if ('repconn_step01' in values and not str(values['repconn_step01']).isspace()):
        REPCONN_STEP01 = values['repconn_step01']
    elif ('REPCONN_STEP01' in values and not str(values['REPCONN_STEP01']).isspace()):
        REPCONN_STEP01 = values['REPCONN_STEP01']
    REPCONN_STEP02 = ""
    if ('repconn_step02' in values and not str(values['repconn_step02']).isspace()):
        REPCONN_STEP02 = values['repconn_step02']
    elif ('REPCONN_STEP02' in values and not str(values['REPCONN_STEP02']).isspace()):
        REPCONN_STEP02 = values['REPCONN_STEP02']
    REPCONN_STEP03 = ""
    if ('repconn_step03' in values and not str(values['repconn_step03']).isspace()):
        REPCONN_STEP03 = values['repconn_step03']
    elif ('REPCONN_STEP03' in values and not str(values['REPCONN_STEP03']).isspace()):
        REPCONN_STEP03 = values['REPCONN_STEP03']
    REPCONN_STEP04 = ""
    if ('repconn_step04' in values and not str(values['repconn_step04']).isspace()):
        REPCONN_STEP04 = values['repconn_step04']
    elif ('REPCONN_STEP04' in values and not str(values['REPCONN_STEP04']).isspace()):
        REPCONN_STEP04 = values['REPCONN_STEP04']
    REPCONN_STEP05 = ""
    if ('repconn_step05' in values and not str(values['repconn_step05']).isspace()):
        REPCONN_STEP05 = values['repconn_step05']
    elif ('REPCONN_STEP05' in values and not str(values['REPCONN_STEP05']).isspace()):
        REPCONN_STEP05 = values['REPCONN_STEP05']
    REPCONN_STEP06 = ""
    if ('repconn_step06' in values and not str(values['repconn_step06']).isspace()):
        REPCONN_STEP06 = values['repconn_step06']
    elif ('REPCONN_STEP06' in values and not str(values['REPCONN_STEP06']).isspace()):
        REPCONN_STEP06 = values['REPCONN_STEP06']
    REPCONN_STEP07 = ""
    if ('repconn_step07' in values and not values['repconn_step07'].isspace()):
        REPCONN_STEP07 = values['repconn_step07']
    elif ('REPCONN_STEP07' in values and not values['REPCONN_STEP07'].isspace()):
        REPCONN_STEP07 = values['REPCONN_STEP07']
    REPCONN_STEP08 = ""
    if ('repconn_step08' in values and not str(values['repconn_step08']).isspace()):
        REPCONN_STEP08 = values['repconn_step08']
    elif ('REPCONN_STEP08' in values and not str(values['REPCONN_STEP08']).isspace()):
        REPCONN_STEP08 = values['REPCONN_STEP08']
    REPCONN_STEP09 = ""
    if ('repconn_step09' in values and not str(values['repconn_step09']).isspace()):
        REPCONN_STEP09 = values['repconn_step09']
    elif ('REPCONN_STEP09' in values and not str(values['REPCONN_STEP09']).isspace()):
        REPCONN_STEP09 = values['REPCONN_STEP09']
    REPCONN_STEP10 = ""
    if ('repconn_step10' in values and not str(values['repconn_step10']).isspace()):
        REPCONN_STEP10 = values['repconn_step10']
    elif ('REPCONN_STEP10' in values and not str(values['REPCONN_STEP10']).isspace()):
        REPCONN_STEP10 = values['REPCONN_STEP10']
    REPCONN_STEP11 = ""
    if ('repconn_step11' in values and not str(values['repconn_step11']).isspace()):
        REPCONN_STEP11 = values['repconn_step11']
    elif ('REPCONN_STEP11' in values and not str(values['REPCONN_STEP11']).isspace()):
        REPCONN_STEP11 = values['REPCONN_STEP11']
    REPCONN_STEP12 = ""
    if ('repconn_step12' in values and not str(values['repconn_step12']).isspace()):
        REPCONN_STEP12 = values['repconn_step12']
    elif ('REPCONN_STEP12' in values and not str(values['REPCONN_STEP12']).isspace()):
        REPCONN_STEP12 = values['REPCONN_STEP12']

    POWLOSS_STEP01 = ""
    if ('powloss_step01' in values and not str(values['powloss_step01']).isspace()):
        POWLOSS_STEP01 = values['powloss_step01']
    elif ('POWLOSS_STEP01' in values and not str(values['POWLOSS_STEP01']).isspace()):
        POWLOSS_STEP01 = values['POWLOSS_STEP01']
    POWLOSS_STEP02 = ""
    if ('powloss_step02' in values and not str(values['powloss_step02']).isspace()):
        POWLOSS_STEP02 = values['powloss_step02']
    elif ('POWLOSS_STEP02' in values and not str(values['POWLOSS_STEP02']).isspace()):
        POWLOSS_STEP02 = values['POWLOSS_STEP02']
    POWLOSS_STEP03 = ""
    if ('powloss_step03' in values and not str(values['powloss_step03']).isspace()):
        POWLOSS_STEP03 = values['powloss_step03']
    elif ('POWLOSS_STEP03' in values and not str(values['POWLOSS_STEP03']).isspace()):
        POWLOSS_STEP03 = values['POWLOSS_STEP03']
    POWLOSS_STEP04 = ""
    if ('powloss_step04' in values and not str(values['powloss_step04']).isspace()):
        POWLOSS_STEP04 = values['powloss_step04']
    elif ('POWLOSS_STEP04' in values and not str(values['POWLOSS_STEP04']).isspace()):
        POWLOSS_STEP04 = values['POWLOSS_STEP04']
    POWLOSS_STEP05 = ""
    if ('powloss_step05' in values and not str(values['powloss_step05']).isspace()):
        POWLOSS_STEP05 = values['powloss_step05']
    elif ('POWLOSS_STEP05' in values and not str(values['POWLOSS_STEP05']).isspace()):
        POWLOSS_STEP05 = values['POWLOSS_STEP05']
    POWLOSS_STEP06 = ""
    if ('powloss_step06' in values and not str(values['powloss_step06']).isspace()):
        POWLOSS_STEP06 = values['powloss_step06']
    elif ('POWLOSS_STEP06' in values and not str(values['POWLOSS_STEP06']).isspace()):
        POWLOSS_STEP06 = values['POWLOSS_STEP06']
    POWLOSS_STEP07 = ""
    if ('powloss_step07' in values and not str(values['powloss_step07']).isspace()):
        POWLOSS_STEP07 = values['powloss_step07']
    elif ('POWLOSS_STEP07' in values and not str(values['POWLOSS_STEP07']).isspace()):
        POWLOSS_STEP07 = values['POWLOSS_STEP07']
    POWLOSS_STEP08 = ""
    if ('powloss_step08' in values and not str(values['powloss_step08']).isspace()):
        POWLOSS_STEP08 = values['powloss_step08']
    elif ('POWLOSS_STEP08' in values and not str(values['POWLOSS_STEP08']).isspace()):
        POWLOSS_STEP08 = values['POWLOSS_STEP08']
    POWLOSS_STEP09 = ""
    if ('powloss_step09' in values and not str(values['powloss_step09']).isspace()):
        POWLOSS_STEP09 = values['powloss_step09']
    elif ('POWLOSS_STEP09' in values and not str(values['POWLOSS_STEP09']).isspace()):
        POWLOSS_STEP09 = values['POWLOSS_STEP09']
    POWLOSS_STEP10 = ""
    if ('powloss_step10' in values and not str(values['powloss_step10']).isspace()):
        POWLOSS_STEP10 = values['powloss_step10']
    elif ('POWLOSS_STEP10' in values and not str(values['POWLOSS_STEP10']).isspace()):
        POWLOSS_STEP10 = values['POWLOSS_STEP10']
    POWLOSS_STEP11 = ""
    if ('powloss_step11' in values and not str(values['powloss_step11']).isspace()):
        POWLOSS_STEP11 = values['powloss_step11']
    elif ('POWLOSS_STEP11' in values and not str(values['POWLOSS_STEP11']).isspace()):
        POWLOSS_STEP11 = values['POWLOSS_STEP11']
    POWLOSS_STEP12 = ""
    if ('powloss_step12' in values and not str(values['powloss_step12']).isspace()):
        POWLOSS_STEP12 = values['powloss_step12']
    elif ('POWLOSS_STEP12' in values and not str(values['POWLOSS_STEP12']).isspace()):
        POWLOSS_STEP12 = values['POWLOSS_STEP12']

    OP_COUNT_STEP1 = ""
    if ('op_count_step1' in values and not str(values['op_count_step1']).isspace()):
        OP_COUNT_STEP1 = values['op_count_step1']
    elif ('OP_COUNT_STEP1' in values and not str(values['OP_COUNT_STEP1']).isspace()):
        OP_COUNT_STEP1 = values['OP_COUNT_STEP1']
    OP_COUNT_STEP2 = ""
    if ('op_count_step2' in values and not str(values['op_count_step2']).isspace()):
        OP_COUNT_STEP2 = values['op_count_step2']
    elif ('OP_COUNT_STEP2' in values and not str(values['OP_COUNT_STEP2']).isspace()):
        OP_COUNT_STEP2 = values['OP_COUNT_STEP2']
    OP_COUNT_STEP3 = ""
    if ('op_count_step3' in values and not str(values['op_count_step3']).isspace()):
        OP_COUNT_STEP3 = values['op_count_step3']
    elif ('OP_COUNT_STEP3' in values and not str(values['OP_COUNT_STEP3']).isspace()):
        OP_COUNT_STEP03 = values['OP_COUNT_STEP03']
    OP_COUNT_STEP4 = ""
    if ('op_count_step4' in values and not str(values['op_count_step4']).isspace()):
        OP_COUNT_STEP4 = values['op_count_step4']
    elif ('OP_COUNT_STEP4' in values and not str(values['OP_COUNT_STEP4']).isspace()):
        OP_COUNT_STEP4 = values['OP_COUNT_STEP4']
    OP_COUNT_STEP5 = ""
    if ('op_count_step5' in values and not str(values['op_count_step5']).isspace()):
        OP_COUNT_STEP5 = values['op_count_step5']
    elif ('OP_COUNT_STEP5' in values and not str(values['OP_COUNT_STEP5']).isspace()):
        OP_COUNT_STEP5 = values['OP_COUNT_STEP5']
    OP_COUNT_STEP6 = ""
    if ('op_count_step06' in values and not str(values['op_count_step06']).isspace()):
        OP_COUNT_STEP6 = values['op_count_step6']
    elif ('OP_COUNT_STEP06' in values and not str(values['OP_COUNT_STEP06']).isspace()):
        OP_COUNT_STEP6 = values['OP_COUNT_STEP6']
    OP_COUNT_STEP7 = ""
    if ('op_count_step7' in values and not str(values['op_count_step7']).isspace()):
        OP_COUNT_STEP7 = values['op_count_step7']
    elif ('OP_COUNT_STEP7' in values and not str(values['OP_COUNT_STEP7']).isspace()):
        OP_COUNT_STEP7 = values['OP_COUNT_STEP7']
    OP_COUNT_STEP8 = ""
    if ('op_count_step8' in values and not str(values['op_count_step8']).isspace()):
        OP_COUNT_STEP8 = values['op_count_step8']
    elif ('OP_COUNT_STEP8' in values and not str(values['OP_COUNT_STEP8']).isspace()):
        OP_COUNT_STEP8 = values['OP_COUNT_STEP8']
    OP_COUNT_STEP9 = ""
    if ('op_count_step9' in values and not str(values['op_count_step9']).isspace()):
        OP_COUNT_STEP9 = values['op_count_step9']
    elif ('OP_COUNT_STEP9' in values and not str(values['OP_COUNT_STEP9']).isspace()):
        OP_COUNT_STEP9 = values['OP_COUNT_STEP9']
    OP_COUNT_STEP10 = ""
    if ('op_count_step10' in values and not str(values['op_count_step10']).isspace()):
        OP_COUNT_STEP10 = values['op_count_step10']
    elif ('OP_COUNT_STEP10' in values and not str(values['OP_COUNT_STEP10']).isspace()):
        OP_COUNT_STEP10 = values['OP_COUNT_STEP10']
    OP_COUNT_STEP11 = ""
    if ('op_count_step11' in values and not str(values['op_count_step11']).isspace()):
        OP_COUNT_STEP11 = values['op_count_step11']
    elif ('OP_COUNT_STEP11' in values and not str(values['OP_COUNT_STEP11']).isspace()):
        OP_COUNT_STEP11 = values['OP_COUNT_STEP11']
    OP_COUNT_STEP12 = ""
    if ('op_count_step12' in values and not str(values['op_count_step12']).isspace()):
        OP_COUNT_STEP12 = values['op_count_step12']
    elif ('OP_COUNT_STEP12' in values and not str(values['OP_COUNT_STEP12']).isspace()):
        OP_COUNT_STEP12 = values['OP_COUNT_STEP12']

    cosPhiDaily = ""
    if ('cosPhiDaily' in values and not str(values['cosPhiDaily']).isspace()):
        cosPhiDaily = values['cosPhiDaily']

    cosPhiWeekly = ""
    if ('cosPhiWeekly' in values and not str(values['cosPhiWeekly']).isspace()):
        cosPhiWeekly = values['cosPhiWeekly']

    print("S3 inside Query") #timestamp is created in insertion time
#    sql = """INSERT INTO metric_data (device_id, OPERATING_HOURS, PH_CON_TOT, PH_GEN_TOT, QH_Q1_TOT, QH_Q2_TOT,QH_Q3_TOT,QH_Q4_TOT,REPAIR_HOURS,V_PP,THDV_PP,THDI,REPCONN_STEP01,REPCONN_STEP02,REPCONN_STEP03,REPCONN_STEP04,REPCONN_STEP05,REPCONN_STEP06,REPCONN_STEP07,REPCONN_STEP08,REPCONN_STEP09,REPCONN_STEP10,REPCONN_STEP11,REPCONN_STEP12,POWLOSS_STEP01,POWLOSS_STEP02,POWLOSS_STEP03,POWLOSS_STEP04,POWLOSS_STEP05,POWLOSS_STEP06,POWLOSS_STEP07,POWLOSS_STEP08,POWLOSS_STEP09,POWLOSS_STEP10,POWLOSS_STEP11,POWLOSS_STEP12,OP_COUNT_STEP1,OP_COUNT_STEP2,OP_COUNT_STEP3,OP_COUNT_STEP4,OP_COUNT_STEP5,OP_COUNT_STEP6,OP_COUNT_STEP7,OP_COUNT_STEP8,OP_COUNT_STEP9,OP_COUNT_STEP10,OP_COUNT_STEP11,OP_COUNT_STEP12, cosPhiDaily, cosPhiWeekly) VALUES (device_id, OPERATING_HOURS, PH_CON_TOT, PH_GEN_TOT, QH_Q1_TOT, QH_Q2_TOT,QH_Q3_TOT,QH_Q4_TOT,REPAIR_HOURS,V_PP,THDV_PP,THDI,REPCONN_STEP01,REPCONN_STEP02,REPCONN_STEP03,REPCONN_STEP04,REPCONN_STEP05,REPCONN_STEP06,REPCONN_STEP07,REPCONN_STEP08,REPCONN_STEP09,REPCONN_STEP10,REPCONN_STEP11,REPCONN_STEP12,POWLOSS_STEP01,POWLOSS_STEP02,POWLOSS_STEP03,POWLOSS_STEP04,POWLOSS_STEP01,POWLOSS_STEP05,POWLOSS_STEP06,POWLOSS_STEP07,POWLOSS_STEP08,POWLOSS_STEP09,POWLOSS_STEP10,POWLOSS_STEP11,POWLOSS_STEP12,OP_COUNT_STEP1,OP_COUNT_STEP2,OP_COUNT_STEP3,OP_COUNT_STEP4,OP_COUNT_STEP5,OP_COUNT_STEP6,OP_COUNT_STEP7,OP_COUNT_STEP8,OP_COUNT_STEP9,OP_COUNT_STEP10,OP_COUNT_STEP11,OP_COUNT_STEP12, cosPhiDaily, cosPhiWeekly)"""
#    sql = "INSERT INTO metric_data (device_id, OPERATING_HOURS, PH_CON_TOT, PH_GEN_TOT, QH_Q1_TOT, QH_Q2_TOT,QH_Q3_TOT,QH_Q4_TOT,REPAIR_HOURS,V_PP,THDV_PP,THDI,REPCONN_STEP01,REPCONN_STEP02,REPCONN_STEP03,REPCONN_STEP04,REPCONN_STEP05,REPCONN_STEP06,REPCONN_STEP07,REPCONN_STEP08,REPCONN_STEP09,REPCONN_STEP10,REPCONN_STEP11,REPCONN_STEP12,POWLOSS_STEP01,POWLOSS_STEP02,POWLOSS_STEP03,POWLOSS_STEP04,POWLOSS_STEP05,POWLOSS_STEP06,POWLOSS_STEP07,POWLOSS_STEP08,POWLOSS_STEP09,POWLOSS_STEP10,POWLOSS_STEP11,POWLOSS_STEP12,OP_COUNT_STEP1,OP_COUNT_STEP2,OP_COUNT_STEP3,OP_COUNT_STEP4,OP_COUNT_STEP5,OP_COUNT_STEP6,OP_COUNT_STEP7,OP_COUNT_STEP8,OP_COUNT_STEP9,OP_COUNT_STEP10,OP_COUNT_STEP11,OP_COUNT_STEP12, cosPhiDaily, cosPhiWeekly) VALUES (%s,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f)"
    sql = "INSERT INTO metric_data (device_id, OPERATING_HOURS, PH_CON_TOT, PH_GEN_TOT, QH_Q1_TOT, QH_Q2_TOT,QH_Q3_TOT,QH_Q4_TOT,REPAIR_HOURS,V_PP,THDV_PP,THDI,REPCONN_STEP01,REPCONN_STEP02,REPCONN_STEP03,REPCONN_STEP04,REPCONN_STEP05,REPCONN_STEP06,REPCONN_STEP07,REPCONN_STEP08,REPCONN_STEP09,REPCONN_STEP10,REPCONN_STEP11,REPCONN_STEP12,POWLOSS_STEP01,POWLOSS_STEP02,POWLOSS_STEP03,POWLOSS_STEP04,POWLOSS_STEP05,POWLOSS_STEP06,POWLOSS_STEP07,POWLOSS_STEP08,POWLOSS_STEP09,POWLOSS_STEP10,POWLOSS_STEP11,POWLOSS_STEP12,OP_COUNT_STEP1,OP_COUNT_STEP2,OP_COUNT_STEP3,OP_COUNT_STEP4,OP_COUNT_STEP5,OP_COUNT_STEP6,OP_COUNT_STEP7,OP_COUNT_STEP8,OP_COUNT_STEP9,OP_COUNT_STEP10,OP_COUNT_STEP11,OP_COUNT_STEP12, cosPhiDaily, cosPhiWeekly) VALUES (%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d)"
    val =(device_id, OPERATING_HOURS, PH_CON_TOT, PH_GEN_TOT, QH_Q1_TOT, QH_Q2_TOT,QH_Q3_TOT,QH_Q4_TOT,REPAIR_HOURS,V_PP,THDV_PP,THDI,REPCONN_STEP01,REPCONN_STEP02,REPCONN_STEP03,REPCONN_STEP04,REPCONN_STEP05,REPCONN_STEP06,REPCONN_STEP07,REPCONN_STEP08,REPCONN_STEP09,REPCONN_STEP10,REPCONN_STEP11,REPCONN_STEP12,POWLOSS_STEP01,POWLOSS_STEP02,POWLOSS_STEP03,POWLOSS_STEP04,POWLOSS_STEP05,POWLOSS_STEP06,POWLOSS_STEP07,POWLOSS_STEP08,POWLOSS_STEP09,POWLOSS_STEP10,POWLOSS_STEP11,POWLOSS_STEP12,OP_COUNT_STEP1,OP_COUNT_STEP2,OP_COUNT_STEP3,OP_COUNT_STEP4,OP_COUNT_STEP5,OP_COUNT_STEP6,OP_COUNT_STEP7,OP_COUNT_STEP8,OP_COUNT_STEP9,OP_COUNT_STEP10,OP_COUNT_STEP11,OP_COUNT_STEP12, cosPhiDaily, cosPhiWeekly)
#    c.execute(sql, val)
    c.execute( """INSERT INTO metric_data (device_id, OPERATING_HOURS, PH_CON_TOT, PH_GEN_TOT, QH_Q1_TOT, QH_Q2_TOT,QH_Q3_TOT,QH_Q4_TOT,REPAIR_HOURS,V_PP,THDV_PP,THDI,REPCONN_STEP01,REPCONN_STEP02,REPCONN_STEP03,REPCONN_STEP04,REPCONN_STEP05,REPCONN_STEP06,REPCONN_STEP07,REPCONN_STEP08,REPCONN_STEP09,REPCONN_STEP10,REPCONN_STEP11,REPCONN_STEP12,POWLOSS_STEP01,POWLOSS_STEP02,POWLOSS_STEP03,POWLOSS_STEP04,POWLOSS_STEP05,POWLOSS_STEP06,POWLOSS_STEP07,POWLOSS_STEP08,POWLOSS_STEP09,POWLOSS_STEP10,POWLOSS_STEP11,POWLOSS_STEP12,OP_COUNT_STEP1,OP_COUNT_STEP2,OP_COUNT_STEP3,OP_COUNT_STEP4,OP_COUNT_STEP5,OP_COUNT_STEP6,OP_COUNT_STEP7,OP_COUNT_STEP8,OP_COUNT_STEP9,OP_COUNT_STEP10,OP_COUNT_STEP11,OP_COUNT_STEP12, cosPhiDaily, cosPhiWeekly) VALUES  ('device_id', 'OPERATING_HOURS','PH_CON_TOT','PH_GEN_TOT','QH_Q1_TOT','QH_Q2_TOT','QH_Q3_TOT','QH_Q4_TOT','REPAIR_HOURS','V_PP','THDV_PP','THDI','REPCONN_STEP01','REPCONN_STEP02','REPCONN_STEP03','REPCONN_STEP04','REPCONN_STEP05','REPCONN_STEP06','REPCONN_STEP07','REPCONN_STEP08','REPCONN_STEP09','REPCONN_STEP10','REPCONN_STEP11','REPCONN_STEP12','POWLOSS_STEP01','POWLOSS_STEP02','POWLOSS_STEP03','POWLOSS_STEP04','POWLOSS_STEP01','POWLOSS_STEP05','POWLOSS_STEP06','POWLOSS_STEP07','POWLOSS_STEP08','POWLOSS_STEP09','POWLOSS_STEP10','POWLOSS_STEP11','POWLOSS_STEP12','OP_COUNT_STEP1','OP_COUNT_STEP2','OP_COUNT_STEP3','OP_COUNT_STEP4','OP_COUNT_STEP5','OP_COUNT_STEP6','OP_COUNT_STEP7','OP_COUNT_STEP8','OP_COUNT_STEP9','OP_COUNT_STEP10','OP_COUNT_STEP11','OP_COUNT_STEP12', 'cosPhiDaily', 'cosPhiWeekly'))"""
    return sql, val;
#    return sql


@click.command()
@click.option('--s3bucket', help='S3 to get data from')
@click.option('--prefix', default="", help='S3 prefix')
@click.option('--filename', default="")
def run(s3bucket, prefix, filename):
    print("Inside run")
    if filename and not filename.isspace():
        with open(filename, 'r') as f:
            body = f.read()
            jsonStr = json.loads(body)
            sql, val= query(jsonStr)

#            c.execute(sql, val)



    if (prefix and not prefix.isspace() and s3bucket and
    not s3bucket.isspace()):
        s3 = boto3.client('s3')
        files = keys(s3bucket, prefix)
        total = 0
        for item in files:
            print(item)
            buf = BytesIO()
            s3.download_fileobj(s3bucket, item, buf)
            json_object = json.loads(buf.getvalue().decode("utf-8"))
            jsonStr = json.dumps(json_object)
            #                with open(json_object, 'r') as f:
            #                    body = f.read()
            print(jsonStr)
            sql, val= query(jsonStr)
            c.execute(sql, val)

    c.commit()
    c.close()

    return {"result": "ok"}

if __name__ == "__main__":
    run()