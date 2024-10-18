'''Use ETL to Extract, Transform and Load Data on bad drivers'''

from mylib.extract import extract
from mylib.transform_load import trans_load
from mylib.query import read_db, create_record, update_ca, delete_ca

# Extract
extract()

# Transform and Load
trans_load()

# Queries
create_record()
read_db()
update_ca()
delete_ca()
