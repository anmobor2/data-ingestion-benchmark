from fastapi import FastAPI, Request
import os
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
    print(await request.json())
    return {"result": "ok"}

@app.post("/auth_on_register")
async def auth_on_register(request: Request):
    print(request.headers)
    print(await request.json())
    return {"result": "ok"}


@app.post("/on_publish")
async def on_publish(request: Request):
    print(request.headers)
    #print(request.data)
    print(await request.json())

    return {"result": "ok"}
