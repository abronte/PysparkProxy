from pyspark_proxy.proxy import Proxy

from pyspark_proxy.sql.readwriter import DataFrameWriter
from pyspark_proxy.sql.column import Column

__all__ = ['DataFrame']

class DataFrame(Proxy):
    _functions = [
            'toJSON', 'registerTempTable', 'createTempView', 'createOrReplaceTempView',
            'createGlobalTempView', 'createOrReplaceGlobalTempView', 'printSchema',
            'explain', 'exceptAll', 'isLocal', 'show', 'checkpoint', 'localCheckpoint',
            'withWatermark', 'hint', 'count', 'collect', 'toLocalIterator', 'limit',
            'take', 'foreach', 'foreachPartition', 'cache', 'persist', 'storageLevel',
            'unpersist', 'coalesce', 'repartition', 'distinct', 'sample', 'samplyBy',
            'randomSplit', 'colRegex', 'alias', 'crossJoin', 'join', 'sortWithinPartitions',
            'sort', 'describe', 'summary', 'head', 'first', 'select', 'selectExpr',
            'filter', 'groupBy', 'rollup', 'cube', 'agg', 'union', 'unionAll',
            'unionByName', 'intersect', 'subtract', 'dropDuplicates', 'dropna', 'fillna',
            'replace', 'approxQuantile', 'corr', 'cov', 'crosstab', 'feqItems',
            'withColumn', 'withColumnRenamed', 'drop', 'toDF', 'toPandas']

    _column_obj = {}
    _columns = []

    def __init__(self, id):
        self._id = id

    @property
    def dtypes(self):
        return self._call(self._id, 'dtypes', ((), {}))

    @property
    def columns(self):
        if self._columns == []:
            self._columns = self._call(self._id, 'columns', ((), {}))

        return self._columns

    @property
    def write(self):
        return DataFrameWriter(self._id, 'write')

    # retrieves column by key lookup
    # df['age']
    def __getitem__(self, item):
        if item not in self.columns:
            raise AttributeError("'%s' object has no attribute '%s'" % (self.__class__.__name__, item))
        else:
            if item not in self._column_obj:
                res = self._get_item(item)
                col = Column(res['id'], self._id, item)
            else:
                col = self._column_obj[item]

            return col

    # retrieves column by attribute
    # df.age
    def __getattr__(self, item):
        if item in self._functions:
            def method(*args, **kwargs):
                return self._call(self._id, item, (args, kwargs))

            return method
        else:
            return self[item]
