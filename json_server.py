import grpc

import df_pb2
import df_pb2_grpc

from concurrent.futures import ThreadPoolExecutor
import time
import json
import ujson
import orjson

def data_wrapper(s):
    '''Wrap a python dict into gRPC object'''
    return df_pb2.DFRow(row_data=s)

def column_data_wrapper(d):
    '''Wrap a dict length is 15 into gRPC object'''
    return df_pb2.ColumnStringRow(**{
        f'column{i + 1}': v
        for i, (_, v) in enumerate(d.items())
    }) # ColumnStringRow(column1=d['column1'], column2=d['column2'] ... )

def chunk_send(out_string):
    max_size = len(out_string)
    offset = 0
    while offset < max_size:
        chunk_s = out_string[offset:offset + chunk_size]
        offset += chunk_size
        yield data_wrapper(chunk_s)

chunk_size = 256 * 1024  # 128 ~ 1024 KB performance 差不多

class DataFrameService(df_pb2_grpc.DataFrameService):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Read JSON by setting
        is_compat = False
        compat = '_compat' if is_compat else ''
        f_size = '800K'  # 800K or 1600K
        f_name = f'./data/JSON_{f_size}{compat}.json'

        self.df = json.load(open(f_name, encoding='utf-8'))

        print('len', len(self.df))
        print(self.df[0])

    # ---------- gRPC methods ----------
    def GetRowJSON(self, request, context):
        print('send_df_json')
        # for row in self.df.iterrows():
        #     yield data_wrapper(row[1].to_json())
        t = time.time()
        for i in range(len(self.df)):
            yield data_wrapper(json.dumps(self.df[i]))
        print('JSON cost', time.time() - t)
    
    def GetRowuJSON(self, request, context):
        print('send_df_ujson')
        t = time.time()
        for i in range(len(self.df)):
            yield data_wrapper(ujson.encode(self.df[i]))
        print('uJSON cost', time.time() - t)

    def GetRoworJSON(self, request, context):
        print('send_df_orjson')
        t = time.time()
        for i in range(len(self.df)):
            yield data_wrapper(orjson.dumps(self.df[i]).decode())
        print('orJSON cost', time.time() - t)

    def GetChunkedJSON(self, request, context):
        print('chunked_json')
        out_string = json.dumps(self.df)
        return chunk_send(out_string)
    
    def GetChunkeduJSON(self, request, context):
        print('chunked_ujson')
        out_string = ujson.dumps(self.df)
        return chunk_send(out_string)
    
    def GetChunkedorJSON(self, request, context):
        print('chunked_orjson')
        out_string = orjson.dumps(self.df).decode()
        return chunk_send(out_string)

    def GetColumnJSON(self, request, context):
        print('send_column_json')
        t = time.time()
        for i in range(len(self.df)):
            yield column_data_wrapper(self.df[i])
        print('JSON cost', time.time() - t)

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
