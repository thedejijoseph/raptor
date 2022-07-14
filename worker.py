
from datetime import datetime, timedelta
from typing import Union

from src.redash_util import q1
from src.db_util import results_insert

def remove_timestamp(date: datetime, text:bool = True) -> Union[datetime, str]:
    if text:
        return f"{date.year}-{date.month}-{date.day}"
    else:
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

    return key, result

def cache_results(key, result):
    """Write query result to the database"""

    status = results_insert(key, result, datetime.now())
    if status > 0:
        # log success
        pass
    else:
        # log error
        pass

def run_service():
    key, result = run_query()
    cache_results(key, result)


if __name__ == "__main__":
    run_service()
