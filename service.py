
from datetime import datetime, timedelta
import logging
from typing import Union, Tuple
from logging import Logger

from rq import Queue

from src.redash_util import q1
from src.db_util import results_insert

from worker import conn

logger = Logger("service.py")
logger.setLevel(logging.DEBUG)

def remove_timestamp(date: datetime, text:bool = True) -> Union[datetime, str]:
    if text:
        return f"{date.year}-{date.month}-{date.day}"
    else:
        return datetime(date.year, date.month, date.day)

def get_lastweek_dates() -> Tuple[datetime, datetime]:
    """Return start and end dates for last week"""

    # get last week's start and end
    today = datetime.today()
    
    lastweek = today - timedelta(days=7)
    lastweek_start = lastweek - timedelta(days=lastweek.weekday())
    lastweek_start = lastweek_start.replace(hour=0, minute=0, second=0, microsecond=0)

    lastweek_end = lastweek_start + timedelta(days=6)
    lastweek_end = lastweek_end.replace(hour=23, minute=59, second=59, microsecond=0)

    return lastweek_start, lastweek_end

def make_cache_key(start:datetime, end:datetime) -> str:
    """Return a concatenation of datetimes as a cache key"""

    key = f"{remove_timestamp(start)}_{remove_timestamp(end)}"
    return key

def run_query():
    lastweek_start, lastweek_end = get_lastweek_dates()
    result = q1.result(lastweek_start, lastweek_end)

    key = make_cache_key(lastweek_start, lastweek_end)

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

def add_service_to_queue():
    logger.info("queuing service")
    q = Queue(connection=conn)
    q.enqueue(run_service)
