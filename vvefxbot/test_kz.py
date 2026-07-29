from datetime import datetime, timezone, timedelta

IST_OFFSET = timedelta(hours=5, minutes=30)

def _parse_hhmm(s: str):
    return datetime.strptime(s, "%H:%M").time()

def _get_asia_killzone_end_utc(now_utc: datetime) -> int:
    now_ist = now_utc + IST_OFFSET
    m = now_ist.month
    d = now_ist.day
    is_summer = False
    if 3 < m < 11:
        is_summer = True
    elif m == 3 and d >= 9:
        is_summer = True
        
    asia_end_str = "07:30" if is_summer else "08:30"
    end_t = _parse_hhmm(asia_end_str)
    
    end_ist = now_ist.replace(hour=end_t.hour, minute=end_t.minute, second=0, microsecond=0)
    end_utc = end_ist - IST_OFFSET
    if end_utc <= now_utc:
        end_utc = now_utc + timedelta(minutes=1)
    return int(end_utc.timestamp())

now_utc = datetime.now(timezone.utc)
end_ts = _get_asia_killzone_end_utc(now_utc)
end_dt = datetime.fromtimestamp(end_ts, tz=timezone.utc)
end_ist = end_dt + IST_OFFSET

print(f"Now UTC: {now_utc}")
print(f"Now IST: {now_utc + IST_OFFSET}")
print(f"End UTC: {end_dt}")
print(f"End IST: {end_ist}")
