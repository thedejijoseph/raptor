
from environs import Env

import redis
from rq import Worker, Queue, Connection


env = Env()
env.read_env()

listen = ['high', 'default', 'low']

redis_url = env('REDIS_URL', default='redis://localhost:6379')

conn = redis.from_url(redis_url)

if __name__ == '__main__':
    with Connection(conn):
        worker = Worker(map(Queue, listen))
        worker.work()
