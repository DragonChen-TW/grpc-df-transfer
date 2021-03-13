import time
import json
import numpy as np
import pandas as pd

data = np.random.rand(5000, 50)
df = pd.DataFrame(data)
print('shape', df.shape)
# print('data', data)
# print('df', df)

# default is ‘columns’
# allowed values are: {‘split’, ‘records’, ‘index’, ‘columns’, ‘values’, ‘table’}.

methods = ['split', 'records', 'index', 'columns', 'values', 'table']
line_methods = ['split', 'records', 'index', 'table']

def print_fnname(func):
    def wrap():
        print('-' * 10, func.__name__, '-' * 10)
        func()
    return wrap

def rround(x):
    return round(x / 10000, 4)


def timeit(func):
    def wrap(*args, **kwargs):
        t = time.time()
        func(*args, **kwargs)
        print('t', time.time() - t)
    return wrap

@timeit
def json_df(m):
    strings = df.to_json(orient=m)
    # print('s', strings)
    print('s_len', rround(len(strings)))

@print_fnname
def test_whole():
    for m in methods:
        print('method', m)
        json_df(m)

@print_fnname
def test_lines():
    print('test_lines')

    for m in line_methods:
        t = time.time()
        print('method', m)
        s_count = 0
        for i in range(df.shape[0]):
            s = df.loc[i, :].to_json(orient=m)
            s_count += len(s)
        print('s_len', rround(s_count))
        print('time', time.time() - t)

@print_fnname
def test_else():
    t = time.time()
    for m in ['split', 'records']:
        print('method', m)

        strings = df.to_json(orient=m)
        data = json.loads(strings)
        if m == 'split':
            data = data['data']

        print('data', data[0])
    
    print('time', time.time() - t)

test_whole()
test_lines()
# test_else()
