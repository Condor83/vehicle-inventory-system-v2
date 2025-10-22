from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import Optional


class BlobStore:
    async def put_text(self, key: str, content: str) -> str:
        raise NotImplementedError


class LocalBlobStore(BlobStore):
    """Filesystem-backed blob store for storing raw scrape artifacts."""

    def __init__(self, root: Optional[str | Path] = None):
        self.root = Path(root or Path.cwd() / "data" / "raw_blobs")
        self.root.mkdir(parents=True, exist_ok=True)

    async def put_text(self, key: str, content: str) -> str:
        path = self.root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        await asyncio.to_thread(path.write_text, content, encoding="utf-8")
        return str(key)

    def build_key(self, job_id: str, dealer_id: int, suffix: str = "md") -> str:
        timestamp = int(time.time() * 1000)
        filename = f"{dealer_id}_{timestamp}.{suffix}"
        return str(Path(job_id) / filename)
