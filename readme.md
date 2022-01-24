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
