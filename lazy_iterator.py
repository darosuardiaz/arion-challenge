import itertools
from functools import reduce

class LazyIterator:
    def __init__(self, iterable, operations=()):
        self._src = iterable
        self._operations = tuple(operations)

    def __iter__(self):
        it = iter(self._src)
        for operation in self._operations:
            it = operation(it)
        return it

    def _extend(self, operation):
        return LazyIterator(self._src, self._operations + (operation,))

    def map(self, fn):
        return self._extend(lambda it: (fn(x) for x in it))

    def filter(self, pred):
        return self._extend(lambda it: (x for x in it if pred(x)))

    def take(self, n):
        return self._extend(lambda it: itertools.islice(it, n))

    def paginate(self, size, page):
        start = (page - 1) * size
        stop = page * size
        return self._extend(lambda it: itertools.islice(it, start, stop))

    def chunk(self, size, include_partial=True):
        def operation(it):
            buf = []
            for x in it:
                buf.append(x)
                if len(buf) == size:
                    yield tuple(buf); buf.clear()
            if include_partial and buf:
                yield tuple(buf)
        return self._extend(operation)

    def reduce(self, fn, init=None):
        it = iter(self)
        if init is None:
            try:
                first = next(it)
            except StopIteration:
                raise TypeError("empty stream with no init")
            return reduce(fn, it, first)
        else:
            return reduce(fn, it, init)
