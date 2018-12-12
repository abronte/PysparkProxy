from pyspark_proxy.proxy import Proxy

__all__ = ['Vector', 'Vectors', 'DenseVector', 'SparseVector']

try:
    import scipy.sparse
    _have_scipy = True
except:
    # No SciPy in environment, but that's okay
    _have_scipy = False

def _convert_to_vector(l):
    if isinstance(l, Vector):
        return l
    elif type(l) in (array.array, np.array, np.ndarray, list, tuple, xrange):
        return DenseVector(l)
    elif _have_scipy and scipy.sparse.issparse(l):
        assert l.shape[1] == 1, "Expected column vector"
        # Make sure the converted csc_matrix has sorted indices.
        csc = l.tocsc()
        if not csc.has_sorted_indices:
            csc.sort_indices()
        return SparseVector(l.shape[0], csc.indices, csc.data)
    else:
        raise TypeError("Cannot convert type %s into Vector" % type(l))

class Vector(Proxy):
    pass

class DenseVector(Proxy):
    pass

class SparseVector(Proxy):
    pass

class Vectors(object):
    @staticmethod
    def sparse(size, *args):
        return SparseVector(size, *args)

    @staticmethod
    def dense(*elements):
        if len(elements) == 1 and not isinstance(elements[0], (float, int, long)):
            # it's list, numpy.array or other iterable object.
            elements = elements[0]
        return DenseVector(elements)

    @staticmethod
    def squared_distance(v1, v2):
        v1, v2 = _convert_to_vector(v1), _convert_to_vector(v2)
        return v1.squared_distance(v2)

    @staticmethod
    def norm(vector, p):
        return _convert_to_vector(vector).norm(p)

    @staticmethod
    def zeros(size):
        return DenseVector(np.zeros(size))

    @staticmethod
    def _equals(v1_indices, v1_values, v2_indices, v2_values):
        """
        Check equality between sparse/dense vectors,
        v1_indices and v2_indices assume to be strictly increasing.
        """
        v1_size = len(v1_values)
        v2_size = len(v2_values)
        k1 = 0
        k2 = 0
        all_equal = True
        while all_equal:
            while k1 < v1_size and v1_values[k1] == 0:
                k1 += 1
            while k2 < v2_size and v2_values[k2] == 0:
                k2 += 1

            if k1 >= v1_size or k2 >= v2_size:
                return k1 >= v1_size and k2 >= v2_size

            all_equal = v1_indices[k1] == v2_indices[k2] and v1_values[k1] == v2_values[k2]
            k1 += 1
            k2 += 1
        return all_equal
