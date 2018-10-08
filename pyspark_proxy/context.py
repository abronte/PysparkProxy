import sys
from pyspark_proxy.proxy import Proxy

__all__ = ['SparkContext']

class SparkContext(Proxy):
    # _jsc is actually a java object with no python counterpart
    # cant be as dynamic as some of the other classes/functions so
    # the actual function calls are scoped out a bit more
    @property
    def _jsc(self):
        self._func_chain = [{'func': '_jsc'}]

        class Jsc():
            def hadoopConfiguration(jsc_self):
                self._func_chain.append({'func': 'hadoopConfiguration', 'args': (), 'kwargs': {}})

                return jsc_self

            def get(jsc_self, *args):
                self._func_chain.append({'func': 'get', 'args': args, 'kwargs': {}})
                return self._call_chain(self._id)

            def set(jsc_self, *args):
                self._func_chain.append({'func': 'set', 'args': args, 'kwargs': {}})
                return self._call_chain(self._id)

        return Jsc()
