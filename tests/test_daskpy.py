import cesium
import cesium.featurize
import cesium.time_series
import dask
import functools
from os.path import expanduser
import sys

import dask2py


def inc(x):
    return 1 + x


def add(x, y):
    return x + y


def test_simple():
    simple_dsk = {'a': 1, 'b': 2, 'c': (inc, 'a'), 'd': (add, 'b', 'c')}
    map_dsk = {'data': [1, 2, 3], 'values': (dask2py.Map(3), inc, 'data'), 'output': (sum, 'values')}
    exp_dsk = {'data': [1, 2, 3],
               'values-0': (inc, (lambda x: x[0], 'data')),
               'values-1': (inc, (lambda x: x[1], 'data')),
               'values-2': (inc, (lambda x: x[2], 'data')),
               'values': (list, ['values-0', 'values-1', 'values-2']),
               'output': (sum, 'values')}
    assert dask.async.get_sync(map_dsk, 'output') == dask.async.get_sync(exp_dsk, 'output')
    assert dask.async.get_sync(dask2py.expand_map(map_dsk), 'output') == dask.async.get_sync(exp_dsk, 'output')

    ns = {'test_dask2py': sys.modules['test_dask2py']}
    code_lines = dask2py.dask2py(map_dsk, 'output')
    exec('\n'.join(code_lines), globals(), ns)
    assert ns['output'] == dask.async.get_sync(map_dsk, 'output')


def test_cesium():
    dsk = {'uris': [expanduser('~/.local/cesium/ts_data/217801.nc'),
                    expanduser('~/.local/cesium/ts_data/224635.nc')],
           'all_time_series': (dask2py.Map(2), cesium.time_series.from_netcdf, 'uris'),
           'all_features': (dask2py.Map(2), functools.partial(cesium.featurize.featurize_single_ts,
                                                      features_to_use=['maximum', 'minimum']),
                            'all_time_series'),
           'computed_fset': (cesium.featurize.assemble_featureset, 'all_features', 'all_time_series')
          }
    dsk_fset = dask.async.get_sync(dask2py.expand_map(dsk), 'computed_fset')

    ns = {'test_dask2py': sys.modules['test_dask2py']}
    code_lines = dask2py.dask2py(dsk, 'computed_fset')
    exec('\n'.join(code_lines), globals(), ns)
    assert ns['computed_fset'] == dsk_fset
