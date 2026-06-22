from datetime import datetime, timezone, timedelta

now = datetime.now(timezone.utc)
cutoff = now - timedelta(minutes=1440)
print(now.isoformat())
print(cutoff.isoformat())
