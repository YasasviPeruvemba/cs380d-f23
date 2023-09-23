FROM ubuntu:20.04

MAINTAINER Sekwon Lee <sklee@cs.utexas.edu> version: 0.1

USER root

ARG DEBIAN_FRONTEND=noninteractive
ENV TZ=Etc/UTC

RUN apt-get update && apt-get install -y git

RUN git clone https://github.com/YasasviPeruvemba/cs380d-f23.git

RUN git checkout kvstore

ENV KVS_HOME /cs380d-f23/project1

# Install dependencies
WORKDIR ${KVS_HOME}/scripts
RUN bash dependencies2.sh

WORKDIR /
