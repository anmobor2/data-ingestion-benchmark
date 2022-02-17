import paho.mqtt.client as mqtt
import click
import boto3
from io import StringIO, BytesIO
import json
import base64


def keys(bucket_name, prefix='/', delimiter='/'):
    prefix = prefix[1:] if prefix.startswith(delimiter) else prefix
    bucket = boto3.resource('s3').Bucket(bucket_name)
    return (_.key for _ in bucket.objects.filter(Prefix=prefix))

@click.command()
@click.option('--host', default="localhost", envvar="MQTT_HOST", help='MQTT Server Host')
@click.option('--port', default=1883, envvar="MQTT_PORT", help='MOQTT Server Port')
@click.option('--token', default='/me/telemetry', envvar="MQTT_TOKEN", help='MOQTT Server Token')
@click.option('--filename')
@click.option('--s3bucket', default="", help='S3 to get data from')
@click.option('--prefix', default="", help='S3 prefix')
def run(host, port, token, filename, prefix, s3bucket):
    client = mqtt.Client()
    client.username_pw_set("mypassword", password=None)
    client.connect(host=host, port=port)
    client.disconnect()
    if( (filename and not filename.isspace())):
        with open(filename, 'r') as f:
            body = f.read()
            print(body)
            print("DENTRO DE S3 a MQTT RUN")
            r = client.publish("/me/telemetry", payload=body)
            print("RESULTADO LLAMADA VERNE",r)
    else: 
        if( (not filename or filename.isspace() )  and prefix and 
           not prefix.isspace() and s3bucket and 
           not s3bucket.isspace()):
            s3 = boto3.client('s3')
            files = keys(s3bucket, prefix)
            total = 0
            for item in files:
                print(item)
                buf = BytesIO()
                s3.download_fileobj(s3bucket, item, buf)
#                s3.download_file('MyBucket', 'hello-remote.txt', 'json.json')
                json_object = json.loads(buf.getvalue().decode("utf-8"))
                jsonStr = json.dumps(json_object)
#                with open(json_object, 'r') as f:
#                    body = f.read()
                print(jsonStr)
                print("S3 DENTRO LLAMADA VERNE S3")
                r = client.publish("/me/telemetry", payload=jsonStr)
                print("S3 RESULTADO LLAMADA VERNE S3",r)
    client.disconnect()

if __name__ == "__main__":
    run()