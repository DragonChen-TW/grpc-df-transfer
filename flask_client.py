import requests
import time

import numpy as np
import orjson
import datatable as dt

base_url = 'http://127.0.0.1:5000/'


class Meter:
    def __init__(self):
        self.get_time = []
        self.read_time = []

    def update(self, get_t, read_t):
        self.get_time.append(get_t)
        self.read_time.append(read_t)

    def stat(self):
        get_mu = np.mean(self.get_time).round(4)
        get_std = np.std(self.get_time).round(4)
        read_mu = np.mean(self.read_time).round(4)
        read_std = np.std(self.read_time).round(4)
        total_mu = get_mu + read_mu
        total_std = get_std + read_std
        return [
            (get_mu, get_std),
            (read_mu, read_std),
            (total_mu, total_std),
        ]

methods = [
    'row_orjson',
    'row_dtcsv',
    'chunked_orjson',
    'chunked_dtcsv',
]
def lambda_dtcsv(data): return dt.fread(text=data)
decode_map = {
    'row_orjson': orjson.loads,
    'row_dtcsv': lambda_dtcsv,
    'chunked_orjson': orjson.loads,
    'chunked_dtcsv': lambda_dtcsv,
}

n_runs = 5

for m in methods:
    print('-' * 10, m, '-' * 10)

    me = Meter()
    url = f'{base_url}/{m}/'
    decode_func = decode_map[m]
    times = []
    for n in range(n_runs):
        t = time.time()
        res = requests.get(url)
        text = res.content.decode()
        if n == 0:
            print('t', len(text))
        get_t = time.time() - t
        t = time.time()

        if m.startswith('row'):
            df = text.split('\r')[:-1]
            df = [decode_func(d) for d in df]
        elif m.startswith('chunk'):
            df = decode_func(text)
        else:
            continue
    
        read_t = time.time() - t
        me.update(get_t, read_t)
    
    keys = ['get  ', 'read ', 'total']
    stat = me.stat()
    for s in zip(keys, stat):
        print(s)
