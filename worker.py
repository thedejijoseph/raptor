
from datetime import datetime, timedelta

from src.redash_util import q1

def remove_timestamp(date: datetime) -> datetime:
    return datetime(date.year, date.month, date.day)

def run_query():

    # get last week's start and end
    today = datetime.today()
    
    lastweek = today - timedelta(days=7)
    lastweek_start = lastweek - timedelta(days=lastweek.weekday())
    lastweek_start = lastweek_start.replace(hour=0, minute=0, second=0, microsecond=0)

    lastweek_end = lastweek_start + timedelta(days=6)
    lastweek_end = lastweek_end.replace(hour=23, minute=59, second=59, microsecond=0)

    result = q1.result(lastweek_start, lastweek_end)

    key = f"{remove_timestamp(lastweek_start)}_{remove_timestamp(lastweek_end)}"

    return {key: result}

def cache_results(key, result):
    pass

def run_service():
    pass

if __name__ == "__main__":
    run_service()

