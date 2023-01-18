from datetime import datetime
#What is imported
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
    return calendar.timegm(dt.utctimetuple())


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
    return requests.post(url, headers=headers, data=compressed)

if __name__ == "__main__":
    write("http://host.docker.internal:9090/api/v1/write", datetime.utcnow(), random.randint(210, 230), "voltage", {"device": "wola", "deviceType": "raption"})
