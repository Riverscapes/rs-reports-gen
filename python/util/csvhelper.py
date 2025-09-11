import os

def est_rows_for_csv_file(csv_path: str) -> int:
    """Estimate number of rows in a CSV file. If file is small (<10MB), count rows. If large, estimate using average size of first 3 data rows."""
    file_size = os.path.getsize(csv_path)
    if file_size < (3 * 1024 * 1024): # 3 MB
        # Accurate count for small files
        with open(csv_path, mode='r', newline='', encoding='utf-8') as f:
            return sum(1 for _ in f) - 1  # subtract 1 for header
    # For large files, estimate using line lengths
    with open(csv_path, mode='r', newline='', encoding='utf-8') as f:
        header_line = f.readline()
        sample_lines = []
        sample_bytes = 0
        for _ in range(3):
            line = f.readline()
            if not line:
                break
            sample_lines.append(line)
            sample_bytes += len(line.encode('utf-8'))
        if not sample_lines:
            return 0
        avg_row_size = sample_bytes / len(sample_lines)
        data_size = file_size - len(header_line.encode('utf-8'))
        if avg_row_size == 0:
            return len(sample_lines)
        est_rows = int(data_size / avg_row_size)
        # If there are only a few rows, just return the count
        if est_rows < len(sample_lines):
            return len(sample_lines)
        return est_rows
