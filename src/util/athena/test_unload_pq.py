"""compare the awswrangler approach using unload to parquet with our own
not huge differences in time between them - sometimes one is faster, sometimes the other
"""

import time
import pandas as pd

from util.athena.athena import athena_unload_pq_to_dataframe, athena_unload_to_dataframe

querystr = 'select * from rpt_rme_pq order by stream_length desc limit 200'


start = time.time()
df = athena_unload_to_dataframe(querystr)
end = time.time()
print(f'athena_unload_to_dataframe completed in {end-start:.2f} seconds')

start = time.time()
df = athena_unload_pq_to_dataframe(querystr)
end = time.time()
print(f'athena_unload_pq_to_dataframe completed in {end-start:.2f} seconds')

print(df)
