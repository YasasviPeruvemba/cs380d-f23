#!/bin/bash

sudo docker image rm $(sudo docker image ls --format '{{.Repository}} {{.ID}}' | grep 'yperuvemba' | awk '{print $2}')

sudo docker build . -f dockerfiles/base.dockerfile -t yperuvemba/cs380dproject:base --network=host
sudo docker push yperuvemba/cs380dproject:base

sudo docker build . -f dockerfiles/client.dockerfile -t yperuvemba/cs380dproject:client --network=host
sudo docker push yperuvemba/cs380dproject:client

sudo docker build . -f dockerfiles/frontend.dockerfile -t yperuvemba/cs380dproject:frontend --network=host
sudo docker push yperuvemba/cs380dproject:frontend

sudo docker build . -f dockerfiles/server.dockerfile -t yperuvemba/cs380dproject:server --network=host
sudo docker push yperuvemba/cs380dproject:server

cd ..
