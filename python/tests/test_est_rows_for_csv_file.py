import time
from util.csvhelper import est_rows_for_csv_file

"""Run the function through a few sample files, also compare to a full scan of rows
I saved the results in a text file.
"""

csv_paths = [
    '/mnt/c/Users/narlorin/Downloads/017f2481-d17e-499a-aa45-febc2019a89a.csv',
    '/mnt/c/Users/narlorin/Downloads/04c4fb1d-f3cf-43f4-a854-f1f093129256.csv',
    '/tmp/tmpyweftx8_.csv',
    '/mnt/c/Users/narlorin/AppData/Local/JetBrains/DataGrip2025.2/log/open-telemetry-metrics.2025-09-09-15-39-33.csv',
]

for path in csv_paths:
    print(f'\nTesting: {path}')
    try:
        start = time.time()
        count = est_rows_for_csv_file(path)
        elapsed = time.time() - start
        print(f'Estimated rows: {count} (took {elapsed:.3f} seconds)')

        # Compare to Full scan
        start_time = time.time()
        with open(path, newline='', encoding='utf-8') as f:
            total_rows = sum(1 for _ in f) - 1  # subtract 1 for header
        end_time = time.time()
        elapsed_time = end_time - start_time

        print(f"[Full scan] Total rows (excluding header): {total_rows}")
        print(f"[Full scan] Time taken: {elapsed_time:.3f} seconds")
        print(f"Difference as percent : {((count-total_rows)/total_rows*100):.1f}%")


    except Exception as e:
        print(f'Error: {e}')