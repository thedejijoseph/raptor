
# created for heroku's scheduler
# add a job to the queue when this module is fired

from service import add_service_to_queue
from service_campaigns import add_campaigns_to_queue

add_service_to_queue()
add_campaigns_to_queue()
