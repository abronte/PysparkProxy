# FROM debian:9
FROM python:2.7-stretch

# install java
RUN apt-get update && \
  apt-get install -y wget openjdk-8-jdk-headless

# install scala
RUN wget -q -O /tmp/scala.deb https://downloads.lightbend.com/scala/2.12.6/scala-2.12.6.deb && \
  dpkg -i /tmp/scala.deb

RUN mkdir -p /srv/pyspark_proxy

WORKDIR /srv/pyspark_proxy

ADD . /srv/pyspark_proxy

RUN pip install pyspark==2.3.1 && \
  pip install -r requirements.txt

ENV PYTHONPATH /srv/pyspark_proxy
