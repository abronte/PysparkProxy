# Pypsark Proxy [![Build Status](https://travis-ci.org/abronte/PysparkProxy.svg?branch=master)](https://travis-ci.org/abronte/PysparkProxy)
***Under active development. Do not use for production use.***

Seamlessly execute pyspark code on remote clusters.

## How it works
Pyspark proxy is made of up a client and server. The client mimics the pyspark api but when objects get created or called a request is made to the API server. The calls the API server receives then calls the actual pyspark APIs.

## What has been implemented
Currently only some basic functionalities with the `SparkContext`, `sqlContext` and `DataFrame` classes have been implemented. See the [tests](https://github.com/abronte/PysparkProxy/tree/master/tests) for more on what is currently working.

## Getting Started
Install pyspark proxy via pip:
```
pip install pysparkproxy
```

First you need to set up the API server with `spark-submit`. This needs to live wherever your spark driver executes from and is what calls the functions in pyspark.

For [example](https://github.com/abronte/PysparkProxy/blob/master/examples/pyspark_proxy_server.py):

```python
import pyspark_proxy.server as server

server.run()
```

Then start the server `spark-submit pyspark_proxy_server.py`

Now you can start a spark context and do some dataframe operations.

```python
from pyspark_proxy import SparkContext
from pyspark_proxy.sql import SQLContext

sc = SparkContext(appName='pyspark_proxy_app')

sc.setLogLevel('ERROR')

sqlContext = SQLContext(sc)

df = sqlContext.read.json('my.json')

print(df.count())
```

Then use the normal python binary to run this `python my_app.py`. This code works the same if you were to run it via `spark-submit`.
