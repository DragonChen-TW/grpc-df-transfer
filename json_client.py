import grpc
import time

import df_pb2
import df_pb2_grpc

import json
import ujson
import orjson
# import pandas as pd
# import datatable as dt
# import base64
# from io import StringIO

import numpy as np

# global variables
n_runs = 5
is_output = False
out_csv = './benchmark.csv'
out_string = 'method, total_mean, total_std\n'

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

def get_df(stub, rpc_f, read_f):
    '''Higher-level logic of row-by-row get dataframe from rpc_f'''
    global out_string
    name = rpc_f._method.decode().split('/')[-1]
    print('-----' * 5, name, '-----' * 5)
    total_time = []

    for i in range(n_runs):
        req = df_pb2.Empty()
        t = time.time()
        response = rpc_f(req)

        count = 0
        total_size = 0
        all_data = []
        for d in response:
            if i == 0 and count == 0:
                print('len', len(d.row_data), 'x ', end='')
                # print('d', d.row_data)
            total_size += len(d.row_data)
            all_data.append(read_f(d.row_data))
            count += 1
        
        t = time.time() - t
        total_time.append(t)

        if i == 0:
            print('row', count)
            # print('row_s', d.row_data)
            print('total', total_size)

    total_mu = np.mean(total_time).round(4)
    total_std = np.std(total_time).round(4)
    print('total  ', total_mu, total_std)

    out_string += f'{name},{total_mu},{total_std}\n'

def get_column(stub, rpc_f):
    '''Higher-level logic of column get dataframe from rpc_f'''
    global out_string
    name = rpc_f._method.decode().split('/')[-1]
    print('-----' * 5, name, '-----' * 5)
    total_time = []

    for i in range(n_runs):
        req = df_pb2.Empty()
        t = time.time()
        response = rpc_f(req)

        count = 0
        # total_size = 0
        all_data = []
        for d in response:
            all_data.append({
                f'column{i}': getattr(d, f'column{i}') for i in range(1, 16)
            })
            count += 1
        
        t = time.time() - t
        total_time.append(t)

        if i == 0:
            print('row', count)
            # print('row_s', d.row_data)
            # print('total', total_size)

    total_mu = np.mean(total_time).round(4)
    total_std = np.std(total_time).round(4)
    print('total  ', total_mu, total_std)
    out_string += f'{name},{total_mu},{total_std}\n'

def get_chunk(stub, rpc_f, read_f):
    '''Higher-level logic of chunked get dataframe from rpc_f'''
    global out_string
    name = rpc_f._method.decode().split('/')[-1]
    print('-----' * 5, name, '-----' * 5)
    m = Meter()
    for i in range(n_runs):
        req = df_pb2.Empty()
        response = rpc_f(req)

        t = time.time()
        response = [r.row_data for r in response]
        text = ''.join(response)
        # print(text[:1000])
        
        if i == 0:
            print('row', len(response))
            print('total', len(text))

        get_t = time.time() - t
        t = time.time()

        data = read_f(text)

        read_t = time.time() - t

        del data

        m.update(get_t, read_t)
    
    keys = ['get  ', 'read ', 'total']
    stat = m.stat()
    # for s in zip(keys, stat):
    #     print(s)
    print('total', stat[-1])
    
    print('l', len(m.get_time))

    out_string += f'{name},{stat[-1][0]},{stat[-1][1]}\n'

def run():
    # with grpc.insecure_channel('localhost:50051') as channel:
    with grpc.insecure_channel('127.0.0.1:30051') as channel:
        stub = df_pb2_grpc.DataFrameServiceStub(channel)
        get_df(stub, stub.GetRowJSON, json.loads)
        get_df(stub, stub.GetRowuJSON, ujson.loads)
        get_df(stub, stub.GetRoworJSON, orjson.loads)

        get_chunk(stub, stub.GetChunkedJSON, json.loads)
        get_chunk(stub, stub.GetChunkeduJSON, ujson.loads)
        get_chunk(stub, stub.GetChunkedorJSON, orjson.loads)

        get_column(stub, stub.GetColumnJSON)
    
    if is_output:
        with open(out_csv, 'w', encoding='utf-8') as f:
            f.write(out_string)

if __name__ == "__main__":
    run()
