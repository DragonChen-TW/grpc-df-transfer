import grpc

import df_pb2
import df_pb2_grpc

from concurrent.futures import ThreadPoolExecutor
import time
import json
import ujson
import orjson
import pandas as pd
import numpy as np
import datatable as dt
import base64

def data_wrapper(s):
    '''Wrap a python dict into gRPC object'''
    return df_pb2.DFRow(row_data=s)

def chunk_send(out_string):
    max_size = len(out_string)
    offset = 0
    while offset < max_size:
        chunk_s = out_string[offset:offset + chunk_size]
        offset += chunk_size
        yield data_wrapper(chunk_s)

chunk_size = 1024 * 1024  # 256 KB

class DataFrameService(df_pb2_grpc.DataFrameService):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        f_size = '2K'
        # f_name = f'./data_{f_size}.csv'
        f_name = f'./data_{f_size}.jay'

        # only suport datatable compatible file format
        self.dt_df = dt.fread(f_name)
        self.df = self.dt_df.to_pandas()

        print(self.df.shape)
        print(self.df[0:5])

    # ---------- gRPC methods ----------
    def GetDFbyJSON(self, request, context):
        print('send_df_json')
        # for row in self.df.iterrows():
        #     yield data_wrapper(row[1].to_json())
        t = time.time()
        for i in range(self.df.shape[0]):
            yield data_wrapper(self.df.loc[i, :].to_json())
        print('JSON cost', time.time() - t)
    
    def GetDFbyuJSON(self, request, context):
        print('send_df_ujson')
        # for row in self.df.iterrows():
        #     yield data_wrapper(ujson.encode(row[1].to_dict()))
        t = time.time()
        for i in range(self.df.shape[0]):
            yield data_wrapper(ujson.encode(self.df.loc[i, :].to_dict()))
        print('uJSON cost', time.time() - t)

    def GetDFbyorJSON(self, request, context):
        print('send_df_orjson')
        # for row in self.df.iterrows():
        #     yield data_wrapper(ujson.encode(row[1].to_dict()))
        t = time.time()
        values = np.ascontiguousarray(self.df.values)
        for i in range(self.df.shape[0]):
            v = values[i]
            s = orjson.dumps(v, option = orjson.OPT_SERIALIZE_NUMPY)
            yield data_wrapper(s.decode())
        print('orJSON cost', time.time() - t)
    
    def GetDFbyCSV(self, request, context):
        print('send_df_csv')
        t = time.time()
        for row in self.df.iterrows():
            yield data_wrapper(row[1].to_csv())
        print('CSV cost', time.time() - t)
    
    def GetDFbydtCSV(self, request, context):
        print('send_df_dtcsv')
        t = time.time()
        for i in range(self.dt_df.shape[0]):
            yield data_wrapper(self.dt_df[i, :].to_csv())
        print('dtCSV cost', time.time() - t)
    
    def GetDFbyJAY(self, request, context):
        print('send_df_jay')
        t = time.time()
        for i in range(self.dt_df.shape[0]):
            yield df_pb2.ByteRow(row_data=self.dt_df[i, :].to_jay())
        print('JSON cost', time.time() - t)


    def GetChunkedJSON(self, request, context):
        print('chunked_json')
        out_string = self.df.to_json()
        return chunk_send(out_string)
    
    def GetChunkeduJSON(self, request, context):
        print('chunked_ujson')
        out_string = ujson.dumps(self.df.to_dict())
        return chunk_send(out_string)
    
    def GetChunkedorJSON(self, request, context):
        print('chunked_orjson')
        out_string = orjson.dumps(np.ascontiguousarray(
            self.df.values), option=orjson.OPT_SERIALIZE_NUMPY).decode()
        return chunk_send(out_string)
    
    def GetChunkedCSV(self, request, context):
        print('chunked_csv')
        out_string = self.df.to_csv()
        return chunk_send(out_string)
    
    def GetChunkeddtCSV(self, request, context):
        print('chunked_dtcsv')
        out_string = self.dt_df.to_csv()
        return chunk_send(out_string)
    
    def GetChunkedFeather(self, request, context):
        print('chunked_feather')
        out_string = self.df.to_feather()
        return chunk_send(out_string)

def serve():
    # using ThreadPool to build server
    server = grpc.server(ThreadPoolExecutor(5))
    # add service(protocol) into server
    df_pb2_grpc.add_DataFrameServiceServicer_to_server(
        DataFrameService(), server)
    server.add_insecure_port('[::]:30051')
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    serve()
