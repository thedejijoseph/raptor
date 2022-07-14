
# from apscheduler.schedulers.blocking import BlockingScheduler

# from service import run_service

# sched = BlockingScheduler()

# @sched.scheduled_job('interval', minutes=3)
# def timed_job():
#     print("something")

# # @sched.scheduled_job('cron', day_of_week='mon-fri', hour=15, minute=30)
# # def scheduled_job():
# #     print('This job is run every weekday at 5pm.')

# sched.start()


import time

import schedule

from service import add_service_to_queue

schedule.every().monday.at("06:00").do(add_service_to_queue)

while True:
    schedule.run_pending()
    time.sleep(1)
