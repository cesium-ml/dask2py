# dask2py

Utility for printing executable Python code from a `dask` graph.

Usage:
```
from dask2py import dask2py
def inc(x):
    return 1 + x
def add(x, y):
    return x + y

dsk = {'a': 1, 'b': 2, 'c': (inc, 'a'), 'd': (add, 'b', 'c')}
print('\n'.join(dask2py(dsk, 'd')))
```
Output:
```
a = 1
b = 2
c = inc(a)
d = add(b, c)
```
