

from pymongo import MongoClient
from environs import Env

env = Env()
env.read_env()

client = MongoClient(env('URI'))

# => databases
SERVER_3 = client['server-3-prod-db']

# => collections

# server_3 collections
ACCOUNTINGENTITIES = SERVER_3['accountingentities']
ACCOUNTINGHISTORIES = SERVER_3['accountinghistories']
MANDATES = SERVER_3['mandates']
PAYMENTS = SERVER_3['payments']
STASHES = SERVER_3['stashes']
TRANSACTIONS = SERVER_3['transactions']
USERS = SERVER_3['users']
