import paho.mqtt.client as mqtt
import click


@click.command()
@click.option('--host', default="localhost", envvar="MQTT_HOST", help='MQTT Server Host')
@click.option('--port', default=1883, envvar="MQTT_PORT", help='MOQTT Server Port')
@click.option('--token', envvar="MQTT_TOKEN", help='MOQTT Server Token')
@click.option('--filename')
def run(host, port, token, filename):
    client = mqtt.Client()
    #client.username_pw_set(token, password=None)
    client.connect(host=host, port=port)
    with open(filename, 'r') as f:
        body = f.read()
        print(body)
        r = client.publish("/telemetry", payload=f.read())
        print(r)
    client.disconnect()

if __name__ == "__main__":
    run()
