#!/bin/bash

sudo docker image rm $(sudo docker image ls --format '{{.Repository}} {{.ID}}' | grep 'yperuvemba' | awk '{print $2}')

cd dockerfiles

sudo docker build . -f base.dockerfile -t yperuvemba/cs380dproject:base --network=host
sudo docker push yperuvemba/cs380dproject:base

sudo docker build . -f client.dockerfile -t yperuvemba/cs380dproject:client --network=host
sudo docker push yperuvemba/cs380dproject:client

sudo docker build . -f frontend.dockerfile -t yperuvemba/cs380dproject:frontend --network=host
sudo docker push yperuvemba/cs380dproject:frontend

sudo docker build . -f server.dockerfile -t yperuvemba/cs380dproject:server --network=host
sudo docker push yperuvemba/cs380dproject:server

cd ..
