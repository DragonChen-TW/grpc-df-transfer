import numpy as np
import pandas as pd
import datatable as dt

print('generating data')

def generate_csv(f_size):
    row_size = {
        '2K': 5,
        '2M': 5000,
        '200M': 500000,
        # '2G': 5000000,
    }
    num_rows = row_size[f_size]
    num_cols = 50

    data = np.random.rand(num_rows, num_cols)

    columns = list(map(lambda n: f'column{n}', range(num_cols)))

    df = pd.DataFrame(data)
    df = dt.Frame(df)

    print(f'writing to {f_size} csv')
    # df.to_csv(f'data/data_{f_size}.csv', index=None)
    
    df.to_csv(f'data/data_{f_size}.csv')
    df.to_jay(f'data/data_{f_size}.jay')


generate_csv('2K')
generate_csv('2M')
generate_csv('200M')
# generate_csv('2G')
