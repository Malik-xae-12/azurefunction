"""
Timezone utilities — all timestamps stored in IST (Asia/Kolkata, UTC+5:30).

Usage:
    from app.utils.timezone import now_ist, IST

    created_at = now_ist()   # datetime with tzinfo=IST
"""

from datetime import datetime, timezone, timedelta

# IST = UTC + 5:30
IST = timezone(timedelta(hours=5, minutes=30), name="IST")


def now_ist() -> datetime:
    """Return the current datetime in IST with tzinfo set."""
    return datetime.now(tz=IST)


def to_ist(dt: datetime) -> datetime:
    """Convert any aware datetime to IST. Naive datetimes are assumed UTC."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(IST)