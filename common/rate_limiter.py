import asyncio
from collections import deque
from time import monotonic


class RateLimiter:
    def __init__(
        self,
        *,
        period: float,
        limit: int,
        interval: float,
        burst: int,
        penalized_status: int = 409,
        penalty_weight: int = 5,
    ) -> None:
        self.period = period
        self.limit = limit
        self.interval = interval
        self.burst = burst
        self.penalized_status = penalized_status
        self.penalty_weight = penalty_weight

        self._period_events: deque[tuple[float, int]] = deque()
        self._interval_events: deque[tuple[float, int]] = deque()
        self._period_total = 0
        self._interval_total = 0
        self._lock = asyncio.Lock()

    def _delete_expired(self, now: float) -> None:
        while self._period_events and now - self._period_events[0][0] >= self.period:
            _, weight = self._period_events.popleft()
            self._period_total -= weight
        while self._interval_events and now - self._interval_events[0][0] >= self.interval:
            _, weight = self._interval_events.popleft()
            self._interval_total -= weight

    def _can_take(self, weight: int) -> bool:
        return (
            (self._period_total + weight) <= self.limit
            and (self._interval_total + weight) <= self.burst
        )

    def _time_until_available(
        self,
        *,
        now: float,
        weight: int,
        events: deque[tuple[float, int]],
        total: int,
        window: float,
        threshold: int,
    ) -> float:
        if total + weight <= threshold:
            return 0.0

        excess = (total + weight) - threshold
        cumulative = 0
        for timestamp, event_weight in events:
            cumulative += event_weight
            if cumulative >= excess:
                return max(0.0, (timestamp + window) - now)
        return window

    def _append(self, now: float, weight: int) -> None:
        self._period_events.append((now, weight))
        self._period_total += weight
        self._interval_events.append((now, weight))
        self._interval_total += weight

    def weight_for_status(self, status: int) -> int:
        return self.penalty_weight if status == self.penalized_status else 1

    async def acquire(self, weight: int = 1) -> None:
        while True:
            async with self._lock:
                now = monotonic()
                self._delete_expired(now)
                if self._can_take(weight):
                    self._append(now, weight)
                    return

                wait_period = self._time_until_available(
                    now=now,
                    weight=weight,
                    events=self._period_events,
                    total=self._period_total,
                    window=self.period,
                    threshold=self.limit,
                )
                wait_interval = self._time_until_available(
                    now=now,
                    weight=weight,
                    events=self._interval_events,
                    total=self._interval_total,
                    window=self.interval,
                    threshold=self.burst,
                )
                wait_for = max(wait_period, wait_interval, 0.01)
            await asyncio.sleep(wait_for)

    async def record_response(self, status: int, reserved_weight: int = 1) -> None:
        actual_weight = self.weight_for_status(status)
        extra_weight = max(0, actual_weight - reserved_weight)
        if not extra_weight:
            return

        async with self._lock:
            now = monotonic()
            self._delete_expired(now)
            self._append(now, extra_weight)
