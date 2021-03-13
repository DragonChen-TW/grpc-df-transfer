from flask import app, Flask, request, Response

import numpy as np
import orjson
import datatable as dt

app = Flask(__name__)

f_name = 'data/data_2M.jay'
print(f_name)

dt_df = dt.fread(f_name)
df = dt_df.to_pandas()
print(df.shape)

chunk_size = 256 * 1024

@app.route('/<string:method>/')
def send(method):
    def row_orjson():
        values = np.ascontiguousarray(df.values)
        for i in range(df.shape[0]):
            v = values[i]
            s = orjson.dumps(v, option=orjson.OPT_SERIALIZE_NUMPY)
            yield s.decode() + '\r'
    def row_dtcsv():
        for i in range(dt_df.shape[0]):
            yield dt_df[i, :].to_csv() + '\r'
    
    def chunk_send(out_string):
        max_size = len(out_string)
        offset = 0
        while offset < max_size:
            chunk_s = out_string[offset:offset + chunk_size]
            offset += chunk_size
            yield chunk_s
    def chunked_orjson():
        out_string = orjson.dumps(
            np.ascontiguousarray(df.values),
            option=orjson.OPT_SERIALIZE_NUMPY
        ).decode()
        return chunk_send(out_string)
    def chunked_dtcsv():
        out_string = dt_df.to_csv()
        return chunk_send(out_string)

    gen_map = {
        'row_orjson': row_orjson,
        'row_dtcsv': row_dtcsv,
        'chunked_orjson': chunked_orjson,
        'chunked_dtcsv': chunked_dtcsv,
    }
    # method = request.args.get('method')
    print('m', method)
    gen = gen_map[method]
    return Response(gen(), mimetype='text/plain')

if __name__ == '__main__':
    app.run()
