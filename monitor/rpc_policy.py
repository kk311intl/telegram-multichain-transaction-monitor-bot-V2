"""Endpoint rate-limit memory and pacing; callers hold their shared health lock."""
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
import math


LIMIT_DELAYS = (60, 120, 300, 600, 900)
LIMIT_INTERVALS = (0, 0.25, 0.5, 1, 2, 4)


def retry_seconds(value, now):
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        try:
            seconds = parsedate_to_datetime(value).timestamp() - now
        except (TypeError, ValueError, OverflowError, AttributeError):
            return 0
    return min(86400, max(0, math.ceil(seconds))) if math.isfinite(seconds) else 0


@dataclass
class RateLimit:
    level: int = 0
    recover_at: float = 0.0
    next_at: float = 0.0

    @property
    def interval(self):
        return LIMIT_INTERVALS[self.level]

    def limited(self, now, retry_after, already_cooling):
        # Parallel replies from one burst count as one step, not five strikes.
        if not already_cooling or not self.level:
            self.level = min(len(LIMIT_DELAYS), self.level + 1)
        delay = max(LIMIT_DELAYS[self.level - 1], retry_after)
        self.recover_at = max(self.recover_at, now + delay + 600)
        return delay

    def succeeded(self, now):
        # Fast responses cannot erase recent 429s; recovery requires real success.
        if self.level and now >= self.recover_at:
            self.level -= 1
            self.recover_at = now + 120

    def snapshot(self, now):
        return {'level': self.level, 'recovery': max(0, self.recover_at-now),
                'spacing': max(0, self.next_at-now)}

    def restore(self, data, now, age):
        self.level = min(len(LIMIT_DELAYS), max(0, int(data.get('level', 0))))
        self.recover_at = now + max(0, min(87000, float(data.get('recovery', 0)))-age)
        self.next_at = now + max(0, min(4, float(data.get('spacing', 0)))-age)
