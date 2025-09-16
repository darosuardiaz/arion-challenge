import sys
import os
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from lazy_iterator import LazyIterator


def is_prime(n: int) -> bool:
    if n < 2: 
        return False
    i = 2
    while i * i <= n:
        if n % i == 0:
            return False
        i += 1
    return True


def test_lazy_iterator_basic_operations():
    """Test basic map, filter, take operations."""
    # Test basic filter and map
    result = list(LazyIterator(range(10))
                 .filter(lambda x: x % 2 == 0)
                 .map(lambda x: x * 2))
    # Even numbers from 0-9: [0, 2, 4, 6, 8] then doubled: [0, 4, 8, 12, 16]
    assert result == [0, 4, 8, 12, 16]
    
    # Test take
    result = list(LazyIterator(range(100)).take(5))
    assert result == [0, 1, 2, 3, 4]


def test_lazy_iterator_chaining():
    """Test complex chaining of operations."""
    seq = (LazyIterator(range(100)) 
           .filter(is_prime)
           .map(lambda x: x * x)
           .take(5))
    
    result = list(seq)
    # First 5 primes: 2, 3, 5, 7, 11
    # Squared: 4, 9, 25, 49, 121
    assert result == [4, 9, 25, 49, 121]


def test_lazy_iterator_reduce():
    """Test reduce operation with and without initial value."""
    # Test with initial value
    total = (LazyIterator(range(10))
             .filter(lambda x: x % 2 == 0)
             .map(lambda x: x * x)
             .reduce(lambda acc, x: acc + x, 0))
    # Even numbers 0,2,4,6,8 squared: 0,4,16,36,64 = 120
    assert total == 120
    
    # Test without initial value
    total = (LazyIterator([1, 2, 3, 4, 5])
             .reduce(lambda acc, x: acc + x))
    assert total == 15


def test_lazy_iterator_reduce_empty_stream():
    """Test reduce with empty stream raises TypeError."""
    with pytest.raises(TypeError, match="empty stream with no init"):
        LazyIterator([]).filter(lambda x: False).reduce(lambda acc, x: acc + x)


def test_lazy_iterator_chunking():
    """Test chunking with and without partial chunks."""
    # Test chunking with partial
    batches = list(LazyIterator(range(1, 17))
                  .filter(lambda x: x % 3 != 0)
                  .chunk(size=4, include_partial=True))
    
    # Numbers not divisible by 3: 1,2,4,5,7,8,10,11,13,14,16
    expected = [(1, 2, 4, 5), (7, 8, 10, 11), (13, 14, 16)]
    assert batches == expected
    
    # Test chunking without partial
    batches = list(LazyIterator(range(1, 16))
                  .filter(lambda x: x % 3 != 0)
                  .chunk(size=4, include_partial=False))
    
    # Should only have complete chunks
    expected = [(1, 2, 4, 5), (7, 8, 10, 11)]
    assert batches == expected


def test_lazy_iterator_pagination():
    """Test pagination functionality."""
    # Test page 1 (first 5 odd numbers)
    page1 = list(LazyIterator(range(1, 20))
                .filter(lambda x: x % 2 == 1)
                .paginate(size=5, page=1))
    assert page1 == [1, 3, 5, 7, 9]
    
    # Test page 2 (next 5 odd numbers)
    page2 = list(LazyIterator(range(1, 20))
                .filter(lambda x: x % 2 == 1)
                .paginate(size=5, page=2))
    assert page2 == [11, 13, 15, 17, 19]
    
    # Test page beyond available data
    page3 = list(LazyIterator(range(1, 10))
                .filter(lambda x: x % 2 == 1)
                .paginate(size=5, page=2))
    assert page3 == []


def test_lazy_iterator_empty_operations():
    """Test operations on empty iterators."""
    empty = LazyIterator([])
    
    assert list(empty.map(lambda x: x * 2)) == []
    assert list(empty.filter(lambda x: True)) == []
    assert list(empty.take(10)) == []
    assert list(empty.chunk(5)) == []
    assert list(empty.paginate(5, 1)) == []


def test_lazy_iterator_single_element():
    """Test operations on single-element iterators."""
    single = LazyIterator([42])
    
    assert list(single.map(lambda x: x * 2)) == [84]
    assert list(single.filter(lambda x: x > 40)) == [42]
    assert list(single.take(10)) == [42]
    assert list(single.chunk(5)) == [(42,)]
    assert list(single.paginate(5, 1)) == [42]
    assert single.reduce(lambda acc, x: acc + x, 0) == 42


def test_lazy_iterator_reusability():
    """Test that LazyIterator can be iterated multiple times."""
    li = LazyIterator(range(5)).map(lambda x: x * 2)
    
    result1 = list(li)
    result2 = list(li)
    
    assert result1 == result2 == [0, 2, 4, 6, 8]


def test_lazy_iterator_edge_cases():
    """Test edge cases and error conditions."""
    # Test take with 0
    assert list(LazyIterator(range(10)).take(0)) == []
    
    # Test pagination with page beyond data
    assert list(LazyIterator(range(5)).paginate(5, 2)) == []
    
    # Test chunk with size 1
    result = list(LazyIterator(range(3)).chunk(1))
    assert result == [(0,), (1,), (2,)]
    
    # Test operations that throw exceptions in the functions
    def always_error(x):
        raise ValueError("test error")
    
    li = LazyIterator(range(3)).map(always_error)
    with pytest.raises(ValueError, match="test error"):
        list(li)


def test_lazy_iterator_with_generators():
    """Test LazyIterator works with generator inputs."""
    def number_generator():
        for i in range(5):
            yield i * i
    
    result = list(LazyIterator(number_generator())
                 .filter(lambda x: x > 5)
                 .take(2))
    assert result == [9, 16]


def test_lazy_iterator_performance_characteristics():
    """Test that LazyIterator is actually lazy."""
    call_count = 0
    
    def expensive_operation(x):
        nonlocal call_count
        call_count += 1
        return x * 2
    
    # Create the iterator but don't consume it
    li = LazyIterator(range(1000)).map(expensive_operation)
    assert call_count == 0  # Should not have called the function yet
    
    # Take only first 3 elements
    result = list(li.take(3))
    assert result == [0, 2, 4]
    assert call_count == 3  # Should only have called function 3 times
