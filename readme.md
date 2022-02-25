# MYC Data Ingestion Benchmark 

## Components

- app: Python Toolkit with a set of utilities
- prosmcale: Prometheus on Timescale
- vernemq: MQTT Broker
- grafana: Grafana

## Python Toolkit

- ps2prom: Small API that exposes a /metrics endpoints based on data from a Power Studio Scada
- psrecorder: small cli tool to scrap data every X minuts from power studio and store it in a S3 bucket
- s3mqtt: small cli tool to push data from s3 to an mqtt broker
- verne2promscale: small verne plugin that pushes data to promscale
- lib: common files to all applications

## Layers

- app: Contains all python applications that are build on a single docker image. All the apis, and cli utilities are in the same docker image
- samples: some sample data
- deployment: All files needed to deploy all the components


## Old documentation

Run ps2prom server

    uvicorn main:app --reload --port=9001

Run prometheus

    prometheus --config.file=prometheus.yml --enable-feature=remote-write-receiver


Start docker container

    docker run -v `pwd`:/root -ti python:3.9 /bin/bash

Check how to run promscale



Requirements (not now)

    brew install docker-slim
    

Requirements Mac
    
    brew install jq

Requirements Linux

    apt-get install jq


Add aws credentials


    export AWS_ACCESS_KEY_ID=XXX
    export AWS_SECRET_KEY_SECRET=xxx

Start verne with docker

    docker run -p 1884:1883 --env-file=deployment/docker/verne.env -h vmq0.local  --platform linux/amd64  vernemq/vernemq

Start verne2promscale without docker or kubernetes, directly with python as api rest using uvicorn in port 8889

    uvicorn app.verne2promscale.main:app  --host 0.0.0.0 --port 8889


Start influxdb

    mkdir influx-data
    docker run \
        --name influxdb \
        -p 8086:8086 \
        --volume $PWD/influx-data:/var/lib/influxdb2 \
        influxdb:2.0.9

    Goto localhost:8086 and set user, password, organization and database. Get admin token


Get data from Thingsboard to S3

    python3 -m app.tbtos3.main \
        --bastion-host=54.171.211.53 --bastion-user=ec2-user --bastion-key=~/cluster-bastion.pem \
        --postgres="postgresql://tsdbadmin:XXXXXX@tsdb-27c921-circutor-f875.a.timescaledb.io:19637/production" \
        --start=2022-02-01 --end=2022-02-02


Start Victoria Metrics

    docker run -it --rm -v $PWD/victoria-metrics-data:/victoria-metrics-data -p 8428:8428 victoriametrics/victoria-metrics

Send data to Victoria Metrics

    python3 -m app.s3todb.prometheus --bucket=XXX --prefix=/ --prometheus-url=http://<victoriametrics-addr>:8428/api/v1/write

Run timescalewide and timescalenarrow

    You can run both with a file as a parameter with all the telemetries inside that file or with a Bucket S3 and Prefix as parameters, also there is a file with the valid id devices list, only the devices inside that file will be processed and the connection uri to timescale. 

    With a file as parameter and the devices list file:
    python -m app.s3totimescalenarrow.main --filename=/home/anmoreno/NEWPLATFORM/data_ingestion/data-ingestion-benchmark/samples/2021-11-01T00_02_01.json --uri="postgres://postgres:admin@127.0.0.1:5434/postgres" --devicesfile=/home/anmoreno/NEWPLATFORM/data_ingestion/data-ingestion-benchmark/samples/devicesfile.txt

    same for wide:
    python -m app.s3totimescalewide.main

    with a bucket s3 and prefix and devices list file:
    python -m app.s3totimescalenarrow.main  --uri="postgres://postgres:admin@127.0.0.1:5434/postgres" --devicesfile=/home/anmoreno/NEWPLATFORM/data_ingestion/data-ingestion-benchmark/samples/devicesfile.txt --prefix=/tbdata/000e00b0-82b2-11ec-9949-7f0fdad2c99c/ --s3bucket=myc-ingestion-sample-recorded-data

    same for wide:
    python -m app.s3totimescalewide.main
