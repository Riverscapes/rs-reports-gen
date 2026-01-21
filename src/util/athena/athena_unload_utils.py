import csv
from pathlib import Path
from urllib.parse import urlparse
from rsxml import Logger


def list_athena_unload_payload_files(root: Path) -> list[Path]:
    """Return local data files for an Athena UNLOAD output, honoring CSV manifests."""
    log = Logger('Get parquet files list')
    if not root.exists():
        raise FileNotFoundError(f"Path not found: {root}")
    if not any(root.iterdir()):  # TODO test what if root is a single parquet file?
        raise FileNotFoundError(f"No files found at: {root}")
    manifest_candidates = sorted(root.glob('*manifest.csv'))

    data_files: list[Path] = []

    if manifest_candidates:
        # (1A) If there is more than one manifest, log an error message and return the results of the first one
        if len(manifest_candidates) > 1:
            log.error(f"Multiple manifests found in folder {root}. Using the first one found, which may not be desired.")

        manifest_path = manifest_candidates[0]
        with manifest_path.open(newline='', encoding='utf-8') as manifest_file:
            reader = csv.reader(manifest_file)
            for row in reader:
                if not row:
                    continue
                candidate = row[0].strip()
                if not candidate:
                    continue
                parsed = urlparse(candidate)
                candidate_name = Path(parsed.path).name if parsed.scheme else Path(candidate).name
                if not candidate_name:
                    continue
                local_path = root / candidate_name
                # Only add if it exists - though if it's in the manifest it SHOULD be there.
                if local_path.exists():
                    data_files.append(local_path)
                else:
                    log.error(f"File in manifest not found on disk: {local_path}")

        # If there is a manifest, but it has no files (or valid files), we return the empty list.
        # We do NOT fall back to listing other files.
        return data_files

    # Fallback: if no manifest is found at all, list all files in directory (excluding metadata/manifests)
    data_files = [
        p for p in sorted(root.iterdir())
        if (
            p.is_file()
            and not p.name.startswith('.')
            and 'manifest' not in p.stem.lower()
        )
    ]

    return data_files
