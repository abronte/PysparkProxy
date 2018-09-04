# Pypsark Proxy [![Build Status](https://travis-ci.org/abronte/PysparkProxy.svg?branch=master)](https://travis-ci.org/abronte/PysparkProxy)
***Under active development. Do not use for production use.***

Seamlessly execute pyspark code on remote clusters.

## How it works
Pyspark proxy is made of up a client and server. The client mimics the pyspark api but when objects get created or called a request is made to the API server. The calls the API server receives then calls the actual pyspark APIs.

## What has been implemented
Currently only some basic functionalities with the `SparkContext`, `sqlContext` and `DataFrame` classes have been implemented. See the [tests](https://github.com/abronte/PysparkProxy/tree/master/tests) for more on what is currently working.
