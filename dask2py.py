"""Print executable code from a dask graph."""


__version__ = '0.0.1'


from collections import OrderedDict
import functools
import dask
from dask.order import order


"""TODO list:
    - print out proper list comp instead of using hacky listmap function
    - properly handle nested tuples? e.g. (list, (add, 1, 2))
    - infer necessary import statements
    - resolve imports like 'import numpy as np' by comparing w/ sys.modules
    - probably lots of other stuff I haven't even thought of
"""


# map(f, vals) <--> [f(x, **kwargs) for x in vals]
def listmap(*args, **kwargs):
    return list(map(*args, **kwargs))
class Map():
    def __init__(self, n):
        self.__name__ = 'listmap'
        self.__qualname__ = 'listmap'
        self.n = n

    def __call__(self, *args, **kwargs):
        return listmap(*args, **kwargs)


def pretty_print(obj, dsk):
    if callable(obj):
        if isinstance(obj, functools.partial):  # TODO handle more generically?
            kwarg_list = ["{}={}".format(key, pretty_print(value, dsk))
                          for key, value in obj.keywords.items()]
            return "functools.partial({}, {})".format(pretty_print(obj.func, dsk),
                                                      ', '.join(kwarg_list))
        elif obj.__module__ not in ['builtins', '__main__']:
            return obj.__module__ + '.' + obj.__qualname__
        else:
            return obj.__qualname__
    elif isinstance(obj, str) and obj in dsk.keys():  # variable name, not a string
        return obj
    else:
        return repr(obj)


def dask2py(dsk, keys):
    """Walk through the dask graph `dsk` and print Python code for each operation instead of
    executing. Based on `dask.async.get_sync`.
    """
    code_lines = []
    if not isinstance(dsk, OrderedDict):
        keyorder = order(dsk)
        dsk = OrderedDict(sorted(dsk.items(), key=lambda x: keyorder[x[0]], reverse=True))
    for key, task in dsk.items():
        # {'x': 1}
        if not isinstance(task, tuple) or not callable(task[0]):
            value = task
            code_lines.append("{} = {}".format(key, value))
        # {'z': (add, 'x', 'y')}
        else:
            func = task[0]
            args = task[1:]
            code_lines.append("{} = {}({})".format(key, pretty_print(func, dsk),
                                                   ', '.join(pretty_print(el, dsk) for el in args)))
    return code_lines


def expand_map(dsk):
    """Expand `map` operations into multiple tasks which can be computed in parallel.

    `{'values': (Map(3), inc, 'inputs'), 'output': (reduce, 'values')}`
    becomes
    `{'values-{uuid-0}': (inc, (lambda x: x[0], 'inputs')),
      'values-{uuid-1}': (inc, (lambda x: x[1], 'inputs')),
      'values-{uuid-2}': (inc, (lambda x: x[2], 'inputs')),
      'values': (list, ['values-{uuid-0}', 'values-{uuid-1}', 'values-{uuid-2}']),
      'output': (reduce, 'values')}`.
    """
    out_dsk = OrderedDict()
    for key, task in dsk.items():
        # {'y': (Map(3), inc, 'x')
        if isinstance(task, tuple) and isinstance(task[0], Map):
            map_token, map_fn, map_iter = task
            for i in range(map_token.n):
                sub_key = key + '-' + str(i)
                sub_task = (map_fn, (lambda x, i=i: x[i], map_iter))
                out_dsk[sub_key] = sub_task
            sub_keys = list(out_dsk.keys())[-map_token.n:]
            out_dsk[key] = (list, sub_keys)
        # {'x': 1} or {'z': (add, 'x', 'y')}
        else:
            out_dsk[key] = task
    return out_dsk
