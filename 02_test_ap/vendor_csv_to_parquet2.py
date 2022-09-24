# csv_to_parquet.py
#pip install pandas pyarrow

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

csv_file = './vendor_dim.csv'
parquet_file = './dim_vendor.parquet'
chunksize = 100

parquet_schema = "";

csv_stream = pd.read_csv(csv_file, sep='\t')
#csv_stream = pd.read_csv(csv_file)

print(csv_stream)
my_columns = ["VendorID", "name"]
table = pa.Table.from_pandas(csv_stream)
print(table)
pq.write_table(table, parquet_file, version="1.0")