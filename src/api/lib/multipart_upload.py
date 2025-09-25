"""Streaming multipart uploader for large files."""

from __future__ import annotations

import mimetypes
import os
import uuid
from typing import Dict, Iterator

import requests
from rsxml import ProgressBar


CRLF = "\r\n"
DEFAULT_CHUNK_SIZE = 1024 * 1024


class MultipartStream:
    """Iterates over multipart/form-data payload without buffering the file."""

    def __init__(
        self,
        file_path: str,
        fields: Dict[str, str | bytes],
        boundary: str,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> None:
        self._file_path = file_path
        self._fields = fields
        self._boundary = boundary
        self._chunk_size = chunk_size
        self._filename = os.path.basename(file_path)
        self._content_type = mimetypes.guess_type(self._filename)[0] or "application/octet-stream"
        self._file_size = os.path.getsize(file_path)
        self.progress = 0
        self.prg = ProgressBar(self._file_size, 50, 'Upload Progress', byte_format=True)

        self._field_parts = [self._encode_field(name, value) for name, value in fields.items()]
        self._file_header = self._encode_file_header()
        self._file_footer = (f"{CRLF}--{boundary}--{CRLF}").encode("utf-8")
        self._length = sum(len(part) for part in self._field_parts) + len(self._file_header) + self._file_size + len(self._file_footer)

    def __len__(self) -> int:  # pragma: no cover - exercised via requests
        return self._length

    def __iter__(self) -> Iterator[bytes]:
        self.progress = 0
        for part in self._field_parts:
            self.progress += len(part)
            self.prg.update(self.progress)
            yield part
        # Yield file header and update progress
        self.progress += len(self._file_header)
        self.prg.update(self.progress)
        yield self._file_header
        with open(self._file_path, "rb") as stream:
            while True:
                chunk = stream.read(self._chunk_size)
                if not chunk:
                    break
                self.progress += len(chunk)
                self.prg.update(self.progress)
                yield chunk
        # Yield file footer and update progress
        self.progress += len(self._file_footer)
        self.prg.update(self.progress)
        yield self._file_footer

    def _encode_field(self, name: str, value: str | bytes) -> bytes:
        if isinstance(value, bytes):
            payload = value
        else:
            payload = str(value).encode("utf-8")
        header = (
            f"--{self._boundary}{CRLF}"
            f'Content-Disposition: form-data; name="{name}"{CRLF}{CRLF}'
        ).encode("utf-8")
        return b"".join([header, payload, CRLF.encode("utf-8")])

    def _encode_file_header(self) -> bytes:
        header = (
            f"--{self._boundary}{CRLF}"
            f'Content-Disposition: form-data; name="file"; filename="{self._filename}"{CRLF}'
            f"Content-Type: {self._content_type}{CRLF}{CRLF}"
        )
        return header.encode("utf-8")


def stream_post_file(
    url: str,
    fields: Dict[str, str | bytes],
    file_path: str,
    timeout: int | float | None = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> requests.Response:
    """POST ``file_path`` to ``url`` using streaming multipart upload."""

    boundary = uuid.uuid4().hex
    stream = MultipartStream(file_path=file_path, fields=fields, boundary=boundary, chunk_size=chunk_size)
    headers = {
        "Content-Type": f"multipart/form-data; boundary={boundary}",
        "Content-Length": str(len(stream)),
    }
    return requests.post(url, data=stream, headers=headers, timeout=timeout)
