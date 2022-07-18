
from rq import Queue

from worker import conn

from src.db_util import results_select
from service import get_lastweek_dates, make_cache_key


from src.campaigns.soft_churn import script as soft_churn


def soft_churn_campaign():
    start, end = get_lastweek_dates()
    last_week = make_cache_key(start, end)
    dataset = results_select(last_week)

    soft_churn.handler(dataset)

campaigns = [
    soft_churn_campaign
]

def add_campaigns_to_queue():
    # log: queuing campaigns
    q = Queue(connection=conn)
    for campaign in campaigns:
        q.enqueue(campaign)
