from fastapi import FastAPI, Request
import os
from app.lib import remotewrite
from datetime import datetime
import json
import base64
app = FastAPI()

REMOTE_WRITE = os.getenv('REMOTE_WRITE')

@app.post("/send-metric")
async def send_metric():
    # Send data to prometheus using remote write
    return {'status': 'OK'}


@app.get("/health")
async def status():
    # Send data to prometheus using remote write
    return {'status': 'OK'}


@app.on_event("startup")
async def startup():
    print('Starting server')

@app.post("/auth_on_publish")
async def auth_on_publish(request: Request):
    print(request.headers)
    body = await request.json()
    print(body)
    return {"result": "ok"}

@app.post("/auth_on_register")
async def auth_on_register(request: Request):
    print(request.headers)
    body = await request.json()
    print("REGISTERING")
    print(body)
    if body['username'] == 'mypassword':
        return {"result": "ok"}
    return {"result": "fail"}


@app.post("/on_publish") #es llamado por cliente.publish
async def on_publish(request: Request):
    print("ON_PUBLISH")
    print(request.headers)
    #print(request.data)
    body = await request.json()
    print(body) # Esto saca por la consola en contenido del json
    # que lee python -m app.s3mqtt.main --filename=samples/20220121T114400.json

    #arrives coded, he the body is decoded 
    json_object = json.loads(base64.b64decode(body['payload']))
    print(json_object)

    url = "http://localhost:9201/write"
    ts = json_object["ts"]
    value=json_object["device"]
    labels = json_object["tags"]
    #iterar values totelsvalues
    metrics = json_object["values"] #for each attribute call
    # remote write
    
    print(url, ts, value, metrics, labels)
    for metric,value in metrics.items():
        type(ts)
        dt = ts[0:13] + ":" + ts[13:15]+ ":" + ts[15:17]

        ts2 = datetime.strptime(dt, '%Y-%m-%dT%H:%M:%S')
        remotewrite.write(url, ts2, value, metric, labels)
    
#    host.docker.internal

    # SEND DATA TO PROMSCALE

    return {"result": "ok"}