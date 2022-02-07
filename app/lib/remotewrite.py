from datetime import datetime
from app.lib.prometheus_pb2 import (
    TimeSeries,
    Label,
    Labels,
    Sample,
    WriteRequest
)
import calendar
import logging
import requests
import snappy
import random

def dt2ts(dt):
    """Converts a datetime object to UTC timestamp
  naive datetime will be considered UTC.
  """
    "2022-01-27T222500"
    dt = dt[0:13] + ":" + dt[13:15]+ ":" + dt[15:17]
    date_time_obj = datetime. strptime(dt, '%Y-%m-%dT%H:%M:%S')
    return calendar.timegm(date_time_obj.utctimetuple())


def write(url, ts, value, metric, labels):
    write_request = WriteRequest()

    series = write_request.timeseries.add()

    # name label always required
    label = series.labels.add()
    label.name = "__name__"
    label.value = metric

    # as many labels you like
    for l,v  in labels.items():
        label = series.labels.add()
        label.name = l
        label.value = v

    sample = series.samples.add()
    sample.value = value  # your count?
    sample.timestamp = dt2ts(ts) * 1000


    uncompressed = write_request.SerializeToString()
    compressed = snappy.compress(uncompressed)

    headers = {
        "Content-Encoding": "snappy",
        "Content-Type": "application/x-protobuf",
        "X-Prometheus-Remote-Write-Version": "0.1.0",
        "User-Agent": "metrics-worker"
    }
    try:
        response = requests.post(url, headers=headers, data=compressed)
        print(response)
    except Exception as e:
        print(e)

if __name__ == "__main__":
    write("http://host.docker.internal:9090/api/v1/write", datetime.utcnow(), random.randint(210, 230), "voltage", {"device": "wola", "deviceType": "raption"})
