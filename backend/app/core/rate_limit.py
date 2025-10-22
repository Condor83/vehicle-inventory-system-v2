import asyncio, time
from typing import Optional

class TokenBucket:
    """Simple token bucket for requests-per-minute rate limiting."""
    def __init__(self, rate_per_minute: int, capacity: Optional[int] = None):
        self.rate = rate_per_minute / 60.0
        self.capacity = capacity or rate_per_minute
        self.tokens = self.capacity
        self.last = time.monotonic()
        self.lock = asyncio.Lock()

    async def acquire(self, n: int = 1):
        async with self.lock:
            while self.tokens < n:
                now = time.monotonic()
                elapsed = now - self.last
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
                self.last = now
                if self.tokens < n:
                    await asyncio.sleep(0.05)
            self.tokens -= n
