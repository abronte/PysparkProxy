import pyspark_proxy as pyspark

from pyspark_proxy import SparkContext
from pyspark_proxy.sql import SQLContext

sc = SparkContext(appName='pyspark_proxy_app')

sc.setLogLevel('ERROR')

sqlContext = SQLContext(sc)

df = sqlContext.read.json('my.json')

print(df)
print(df.count())

df.createOrReplaceTempView('foo')

query_df = sqlContext.sql('select count(*) from foo')

print(query_df)

collected = query_df.collect()

print(collected)

selected_df = df.select('foo')

print(selected_df.count())

df = sqlContext.read.csv('my.csv')

print(df.count())

df.show()
