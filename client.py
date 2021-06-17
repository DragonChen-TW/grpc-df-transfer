import grpc
import time

import df_pb2
import df_pb2_grpc

import json
import ujson
import orjson
import pandas as pd
import datatable as dt
import base64
from io import StringIO

import numpy as np

# global variables
n_runs = 5

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
    total_time = []

    for i in range(n_runs):
        req = df_pb2.Empty()
        t = time.time()
        response = rpc_f(req)

        count = 0
        total_size = 0
        for d in response:
            if i == 0 and count == 0:
                print('len', len(d.row_data), 'x ', end='')
                # print('d', d.row_data)
            total_size += len(d.row_data)
            row = read_f(d.row_data)
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

def get_chunk(stub, rpc_f, read_f):
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
    for s in zip(keys, stat):
        print(s)
    
    print('l', len(m.get_time))

def run():
    # with grpc.insecure_channel('localhost:50051') as channel:
    with grpc.insecure_channel('127.0.0.1:30051') as channel:
        stub = df_pb2_grpc.DataFrameServiceStub(channel)
        print('-----' * 5, 'GetRowJSON', '-----' * 5)
        get_df(stub, stub.GetRowJSON, json.loads)

        print('-----' * 5, 'GetRowuJSON', '-----' * 5)
        get_df(stub, stub.GetRowuJSON, ujson.loads)

        print('-----' * 5, 'GetRoworJSON', '-----' * 5)
        get_df(stub, stub.GetRoworJSON, orjson.loads)

        print('-----' * 5, 'GetRowCSV', '-----' * 5)
        def lambda_csv(data): return pd.DataFrame(data.split(','))
        get_df(stub, stub.GetRowCSV, lambda_csv)

        print('-----' * 5, 'GetRowdtCSV', '-----' * 5)
        def lambda_dtcsv(data): return dt.fread(text=data)
        get_df(stub, stub.GetRowdtCSV, lambda_dtcsv)
    
        print('-----' * 5, 'GetRowJAY', '-----' * 5)
        get_df(stub, stub.GetRowJAY, dt.fread)


        print('-----' * 5, 'GetChunkedJSON', '-----' * 5)
        get_chunk(stub, stub.GetChunkedJSON, json.loads)

        print('-----' * 5, 'GetChunkeduJSON', '-----' * 5)
        get_chunk(stub, stub.GetChunkeduJSON, ujson.loads)

        print('-----' * 5, 'GetChunkedorJSON', '-----' * 5)
        get_chunk(stub, stub.GetChunkedorJSON, orjson.loads)
    
        print('-----' * 5, 'GetChunkedCSV', '-----' * 5)
        def lambda_chunk_csv(data):
            return pd.DataFrame([row.split(',') for row in data.split('\n')])
        get_chunk(stub, stub.GetChunkedCSV, lambda_chunk_csv)

        print('-----' * 5, 'GetChunkeddtCSV', '-----' * 5)
        def lambda_chunk_dtcsv(data): return dt.fread(text=data)
        get_chunk(stub, stub.GetChunkeddtCSV, lambda_chunk_dtcsv)

if __name__ == "__main__":
    run()
