import sys
import os

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

print("testing iterator...")
seq = (LazyIterator(range(10_000_000)) 
       .filter(is_prime)
       .map(lambda x: x * x)
       .take(5))

print("result: %s \n" % list(seq))


# reduce
print("testing reducer...")
total_of_first_100_even_squares = (
    LazyIterator(range(10**9))
    .filter(lambda x: x % 2 == 0)
    .map(lambda x: x * x)
    .take(10)
    .reduce(lambda acc, x: acc + x, 0)
)
print("result: %s \n" % total_of_first_100_even_squares)

# chunking
print("testing chunking...")
batches = (
    LazyIterator(range(1, 17))
    .filter(lambda x: x % 3 != 0)
    .chunk(size=4, include_partial=True)
)
print("result:")
for batch in batches:
    print(batch)
# (1, 2, 4, 5)
# (7, 8, 10, 11)
# (13, 14, 16)


# pagination
print("testing pagination...")
page3 = (
    LazyIterator(range(1, 10**9))
    .filter(lambda x: x % 2 == 1)
    .paginate(size=5, page=3)
)

print("result: %s" % list(page3))  
# [21, 23, 25, 27, 29]
