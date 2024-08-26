from datetime import datetime
from pytz import timezone

def convert_to_local_time(utc_time_str, timezone_str):
    utc_time = datetime.strptime(utc_time_str, "%d-%m-%Y %H:%M")
    local_tz = timezone(timezone_str)
    local_time = utc_time.astimezone(local_tz)
    return local_time
