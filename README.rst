Pypsark Proxy |Build Status| |PyPi|
============================

**Under active development. Do not use for production use.**

Seamlessly execute pyspark code on remote clusters.

How it works
------------

Pyspark proxy is made of up a client and server. The client mimics the
pyspark api but when objects get created or called a request is made to
the API server. The calls the API server receives then calls the actual
pyspark APIs.

What has been implemented
-------------------------

Currently only some basic functionalities with the ``SparkContext``,
``sqlContext`` and ``DataFrame`` classes have been implemented. See the
`tests`_ for more on what is currently working.

Getting Started
---------------

Pyspark Proxy requires set up a server where your Spark is located and
simply install the package locally where you want to execute code from.

On Server
~~~~~~~~~

Install pyspark proxy via pip:

::

   pip install pysparkproxy

Start the server:

::

   pyspark-proxy-server start


The server listens on ``localhost:8765`` by default. Check the ``pyspark-proxy-server`` help for additional options.

Locally
~~~~~~~

Install pyspark proxy via pip:

::

   pip install pysparkproxy

Now you can start a spark context and do some dataframe operations.

::

   from pyspark_proxy import SparkContext
   from pyspark_proxy.sql import SQLContext

   sc = SparkContext(appName='pyspark_proxy_app')

   sc.setLogLevel('ERROR')

   sqlContext = SQLContext(sc)

   df = sqlContext.read.json('my.json')

   print(df.count())

Then use the normal python binary to run this ``python my_app.py``. This
code works the same if you were to run it via ``spark-submit``.

.. _tests: https://github.com/abronte/PysparkProxy/tree/master/tests
.. _example: https://github.com/abronte/PysparkProxy/blob/master/examples/pyspark_proxy_server.py

.. |Build Status| image:: https://travis-ci.org/abronte/PysparkProxy.svg?branch=master
   :target: https://travis-ci.org/abronte/PysparkProxy

.. |PyPi| image:: https://img.shields.io/pypi/v/pysparkproxy.svg
   :target: https://pypi.org/project/PysparkProxy/
