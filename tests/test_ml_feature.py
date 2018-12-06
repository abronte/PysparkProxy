import unittest

from base_test_case import BaseTestCase

from pyspark_proxy.sql.dataframe import DataFrame
from pyspark.sql import Row
from pyspark_proxy.ml.feature import *
from pyspark_proxy.ml.linalg import *

class MLFeatureTestCase(BaseTestCase):
    def test_ml_feature_binarizer(self):
        df = self.sqlContext.createDataFrame([(0.5,)], ["values"])
        binarizer = Binarizer(threshold=1.0, inputCol="values", outputCol="features")

        self.assertEqual(binarizer.transform(df).head().features, 0.0)

    def test_ml_feature_bucketizer(self):
        values = [(0.1,), (0.4,), (1.2,), (1.5,), (float("nan"),), (float("nan"),)]
        df = self.sqlContext.createDataFrame(values, ["values"])

        bucketizer = Bucketizer(splits=[-float("inf"), 0.5, 1.4, float("inf")],
            inputCol="values", outputCol="buckets")

        bucketed = bucketizer.setHandleInvalid("keep").transform(df).collect()

        self.assertEqual(len(bucketed), 6)

    def test_ml_feature_count_vectorizer(self):
        df = self.sqlContext.createDataFrame(
                [(0, ["a", "b", "c"]), (1, ["a", "b", "b", "c", "a"])],["label", "raw"])

        cv = CountVectorizer(inputCol="raw", outputCol="vectors")
        model = cv.fit(df)
        model.transform(df)

        self.assertEqual(sorted(model.vocabulary), ['a', 'b', 'c'])

    def test_ml_feature_hashing_tf(self):
        df = self.sqlContext.createDataFrame([(["a", "b", "c"],)], ["words"])
        hashingTF = HashingTF(numFeatures=10, inputCol="words", outputCol="features")
        features = hashingTF.transform(df)

        self.assertEqual(type(features), DataFrame)

    def test_ml_feature_regex_tokenizer(self):
        df = self.sqlContext.createDataFrame([("A B  c",)], ["text"])
        reTokenizer = RegexTokenizer(inputCol="text", outputCol="words")

        row = reTokenizer.transform(df).head()

        self.assertEqual(row.words, [u'a', u'b', u'c'])

    def test_ml_feature_tokenizer(self):
        df = self.sqlContext.createDataFrame([("a b c",)], ["text"])
        tokenizer = Tokenizer(inputCol="text", outputCol="words")

        row = tokenizer.transform(df).head()

        self.assertEqual(row.words, [u'a', u'b', u'c'])

    def test_ml_features_stop_words_remover(self):
        df = self.sqlContext.createDataFrame([(["a", "b", "c"],)], ["text"])
        remover = StopWordsRemover(inputCol="text", outputCol="words", stopWords=["b"])

        self.assertEqual(remover.transform(df).head().words, ['a', 'c'])

    def test_ml_feature_string_indexer(self):
        data = self.sc.parallelize([Row(id=0, label="a"), Row(id=1, label="b"),
                               Row(id=2, label="c"), Row(id=3, label="a"),
                               Row(id=4, label="a"), Row(id=5, label="c")], 2)

        stringIndDf = self.sqlContext.createDataFrame(data, ['id', 'label'])

        stringIndexer = StringIndexer(inputCol="label", outputCol="indexed", handleInvalid="error",
                stringOrderType="frequencyDesc")

        model = stringIndexer.fit(stringIndDf)
        td = model.transform(stringIndDf)

        res = sorted(set([(i[0], i[1]) for i in td.select(td.id, td.indexed).collect()]), key=lambda x: x[0])

        self.assertEqual(res, [(0, 0.0), (1, 2.0), (2, 1.0), (3, 0.0), (4, 0.0), (5, 1.0)])

        inverter = IndexToString(inputCol="indexed", outputCol="label2", labels=model.labels)
        itd = inverter.transform(td) 

        res = sorted(set([(i[0], str(i[1])) for i in itd.select(itd.id, itd.label2).collect()]), key=lambda x: x[0])

        self.assertEqual(res, [(0, 'a'), (1, 'b'), (2, 'c'), (3, 'a'), (4, 'a'), (5, 'c')])

    def test_ml_feature_idf(self):
        df = self.sqlContext.createDataFrame([(DenseVector([1.0, 2.0]),),(DenseVector([0.0, 1.0]),),(DenseVector([3.0, 0.2]),)], ["tf"])

        idf = IDF(minDocFreq=3, inputCol="tf", outputCol="idf")
        model = idf.fit(df)
        res = model.transform(df).head().idf

        self.assertEqual(repr(res), 'DenseVector([0.0, 0.0])')

if __name__ == '__main__':
    unittest.main()
