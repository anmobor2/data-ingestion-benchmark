from fastapi import FastAPI
from app.lib import PowerStudio
import time
from fastapi.responses import PlainTextResponse
app = FastAPI()

ps = PowerStudio("https://powerstudio.circutor.com")

devices = ["0CR02-0", "0CR04-1.AC-Meter", "0CR02-12.PLUG A - Meter", "4CR02-13.PLUG A - Meter", "0CR04-3.PLUG A - Meter", "4CR02-13.EVSE.PLUG A"]


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/single-metrics/{id}", response_class=PlainTextResponse)
async def single_metrics(id: str):
    return ps.get_prometheus(id)


@app.get("/hello/{name}")
async def say_hello(name: str):
    return {"message": f"Hello {name}"}


@app.get("/metrics", response_class=PlainTextResponse)
async def metrics():
    epoch = time.time()
    output = ""
    for device in devices:
        try:
            output = output + ps.get_prometheus(device)
        except Exception as e:
            print('ERROR GETTING: ', device, e)
    print('Total request time', time.time() - epoch)
    return output

