import csv
from pathlib import Path
from urllib.parse import urlparse


def list_unload_payload_files(root: Path) -> list[Path]:
    """Return local data files for an Athena UNLOAD output, honoring CSV manifests."""
    manifest_candidates = sorted(root.glob('*manifest*.csv'))
    data_files: list[Path] = []

    for manifest_path in manifest_candidates:
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
                if local_path.exists():
                    data_files.append(local_path)
        if data_files:
            break

    if not data_files:
        data_files = [
            p for p in sorted(root.iterdir())
            if (
                p.is_file()
                and not p.name.startswith('.')
                and 'manifest' not in p.stem.lower()
            )
        ]

    return data_files
