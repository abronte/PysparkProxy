from pyspark_proxy.proxy import Proxy

__all__ = [
    'Binarizer', 'Bucketizer', 'CountVectorizer', 'CountVectorizerModel', 'HashingTF',
    'IDF', 'RegexTokenizer', 'Tokenizer', 'StringIndexer', 'StringIndexerModel', 'IndexToString',
    'StopWordsRemover']

class Binarizer(Proxy):
    pass

class Bucketizer(Proxy):
    pass

class CountVectorizer(Proxy):
    pass

class CountVectorizerModel(Proxy):
    def __init__(self, id):
        self._id = id

    @property
    def vocabulary(self):
        return self._call(self._id, 'vocabulary', ((), {}))

class HashingTF(Proxy):
    pass

class IDF(Proxy):
    pass

class Tokenizer(Proxy):
    pass

class RegexTokenizer(Proxy):
    pass

class StopWordsRemover(Proxy):
    pass

class StringIndexer(Proxy):
    pass

class StringIndexerModel(Proxy):
    def __init__(self, id):
        self._id = id

    @property
    def labels(self):
        return self._call(self._id, 'labels', ((), {}))

class IndexToString(Proxy):
    pass
