from pyspark_proxy.proxy import Proxy

__all__ = [
    'Binarizer', 'Bucketizer', 'BucketedRandomProjectionLSH', 'BucketedRandomProjectionLSHModel',
    'CountVectorizer', 'CountVectorizerModel',
    'DCT',
    'ElementwiseProduct',
    'FeatureHasher',
    'HashingTF',
    'IDF', 'Imputer', 'IndexToString',
    'MaxAbsScaler', 'MaxAbsScalerModel', 'MinMaxScaler', 'MinMaxScalerModel',
    'NGram', 'Normalizer',
    'OneHotEncoder', 'OneHotEncoderEstimator', 'OneHotEncoderModel',
    'PolynomialExpansion',
    'QuantileDiscretizer',
    'RegexTokenizer',
    'SQLTransformer', 'StandardScaler', 'StandardScalerModel', 'StopWordsRemover', 'StringIndexer', 'StringIndexerModel',
    'Tokenizer',
    'VectorAssembler', 'VectorIndexer', 'VectorIndexerModel', 'VectorSlicer',
    'Word2Vec', 'Word2VecModel',
    'PCA', 'PCAModel',
    'RFormula', 'RFormulaModel',
    'ChiSqSelector', 'ChiSqSelectorModel']

class Binarizer(Proxy):
    pass

class Bucketizer(Proxy):
    pass

class BucketedRandomProjectionLSH(Proxy):
    pass

class BucketedRandomProjectionLSHModel(Proxy):
    pass

class CountVectorizer(Proxy):
    pass

class CountVectorizerModel(Proxy):
    @property
    def vocabulary(self):
        return self._call(self._id, 'vocabulary', ((), {}))

class DCT(Proxy):
    pass

class ElementwiseProduct(Proxy):
    pass

class FeatureHasher(Proxy):
    pass

class HashingTF(Proxy):
    pass

class IDF(Proxy):
    pass

class IDFModel(Proxy):
    @property
    def idf(self):
        return self._call(self._id, 'idf', ((), {}))

class Imputer(Proxy):
    pass

class IndexToString(Proxy):
    pass

class ImputerModel(Proxy):
    @property
    def surrogateDF(self):
        return self._call(self._id, 'surrogateDF', ((), {}))

class MaxAbsScaler(Proxy):
    pass

class MaxAbsScalerModel(Proxy):
    @property
    def maxAbs(self):
        return self._call(self._id, 'maxAbs', ((), {}))

class MinHashLSH(Proxy):
    pass

class MinHashLSHModel(Proxy):
    pass

class MinMaxScaler(Proxy):
    pass

class MinMaxScalerModel(Proxy):
    @property
    def originalMin(self):
        return self._call(self._id, 'originalMin', ((), {}))

    @property
    def originalMax(self):
        return self._call(self._id, 'originalMax', ((), {}))

class NGram(Proxy):
    pass

class Normalizer(Proxy):
    pass

class OneHotEncoder(Proxy):
    pass

class OneHotEncoderEstimator(Proxy):
    pass

class OneHotEncoderModel(Proxy):
    @property
    def categorySizes(self):
        return self._call(self._id, 'categorySizes', ((), {}))

class PolynomialExpansion(Proxy):
    pass

class QuantileDiscretizer(Proxy):
    pass

class RegexTokenizer(Proxy):
    pass

class SQLTransformer(Proxy):
    pass

class StandardScaler(Proxy):
    pass


class StandardScalerModel(Proxy):
    @property
    def std(self):
        return self._call(self._id, 'std', ((), {}))

    @property
    def mean(self):
        return self._call(self._id, 'mean', ((), {}))

class StopWordsRemover(Proxy):
    pass

class StringIndexer(Proxy):
    pass

class StringIndexerModel(Proxy):
    @property
    def labels(self):
        return self._call(self._id, 'labels', ((), {}))

class Tokenizer(Proxy):
    pass

class VectorAssembler(Proxy):
    pass

class VectorIndexer(Proxy):
    pass

class VectorIndexerModel(Proxy):
    @property
    def numFeatures(self):
        return self._call(self._id, 'numFeatures', ((), {}))

    @property
    def categoryMaps(self):
        return self._call(self._id, 'categoryMaps', ((), {}))

class VectorSlicer(Proxy):
    pass

class Word2Vec(Proxy):
    pass

class Word2VecModel(Proxy):
    pass

class PCA(Proxy):
    pass

class PCAModel(Proxy):
    @property
    def pc(self):
        return self._call(self._id, 'pc', ((), {}))

    @property
    def explainedVariance(self):
        return self._call(self._id, 'explainedVariance', ((), {}))

class RFormula(Proxy):
    def __str__(self):
        return self._call(self._id, '__str__', ((), {}))

class RFormulaModel(Proxy):
    def __str__(self):
        return self._call(self._id, '__str__', ((), {}))

class ChiSqSelector(Proxy):
    pass

class ChiSqSelectorModel(Proxy):
    @property
    def selectedFeatures(self):
        return self._call(self._id, 'selectedFeatures', ((), {}))

class VectorSizeHint(Proxy):
    pass
