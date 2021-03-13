import pandas as pd
import datatable as dt

import base64
from io import StringIO

str_data = 'a,b,c\n1,2,3\nTrue,False,True'

# read from string using StringIO
s = StringIO()
s.write(str_data)
s.seek(0) # move pointer to the beginning

df = pd.read_csv(s)

# directly read from string to datatable
dt_df = dt.Frame(str_data)

# encode into jay
jay_data = dt_df.to_jay()
print(jay_data)
# dt_df = dt.fread(jay_data)

b64_data = base64.b64encode(jay_data)
print(b64_data)

deb64_data = base64.b64decode(b64_data)
print(deb64_data)

dt_df = dt.fread(deb64_data)
print(dt_df)