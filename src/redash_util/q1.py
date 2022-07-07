
from datetime import datetime

import pandas as pd

from ..redash_util import USERS as users

DESCRIPTION = """Query: Get list of users that meet these conditions:
    
    - The user signed up between `start_date` and `end_date`
    - They had previously saved with SAYI, Cron, or manually
    - Their last payment was more than 7 days ago
    - They have deleted all of their mandates (mandate.count = mandate.deletedCount)
    - The last mandate activity was more than 7 days ago
    """

def info():
    payload = {
        "description": DESCRIPTION
    }

    return payload

def result(start_date: datetime, end_date: datetime):
    f"""{DESCRIPTION}
    """

    pipeline = [
            {
                "$match": {
                    "$and": [
                        {"createdAt": {"$gte": start_date}},
                        {"createdAt": {"$lte": end_date}}
                    ]
                }
            },
            {
                "$lookup": {
                    "from": "payments",
                    "let": {"uid": "$_id"},
                    "pipeline": [
                        {"$match": {"$expr": {"$eq": ["$uid", "$$uid"]}}},
                        {"$match": {"$expr": {"$eq": ["$category", "charge"]}}},
                        {"$match": {"$expr": {"$eq": ["$status", "success"]}}},
                        {"$group": {
                            "_id": None,
                            "count": {"$sum": 1},
                            "activatedMandate": {"$sum": {"$cond": [{"$eq": ["$type", "mandate_activation_payment"]}, 1, 0]}},
                            "savedWithCashReserve": {"$sum": {"$cond": [{"$eq": ["$type", "cash_reserve_savings"]}, 1, 0]}},
                            "savedWithMandate": {"$sum": {"$cond": [{"$eq": ["$type", "mandate_cron_savings"]}, 1, 0]}},
                            "savedWithSAYI": {"$sum": {"$cond": [{"$eq": ["$type", "mandate_subscription_savings"]}, 1, 0]}},
                            "savedManually": {"$sum": {"$cond": [{"$eq": ["$type", "mandate_manual_savings"]}, 1, 0]}},
                            "lastActivity": {"$last": "$updatedAt"}
                        }}
                    ],
                    "as": "payment"
                }
            },
            {
                "$unwind": {
                    "path": "$payment",
                    "preserveNullAndEmptyArrays": True
                }
            },
            {
                "$lookup": {
                    "from": "mandates",
                    "let": {"uid": "$_id"},
                    "pipeline": [
                        {"$match": {"$expr": {"$eq": ["$uid", "$$uid"]}}},
                        {"$group": {
                            "_id": None,
                            "count": {"$sum": 1},
                            "activeCount": {"$sum": {"$cond": [{"$eq": ["$active", True]}, 1, 0]}},
                            "deletedCount": {"$sum": {"$cond": [{"$eq": ["$isDeleted", True]}, 1, 0]}},
                            "lastActivity": {"$last": "$updatedAt"}
                        }}
                    ],
                    "as": "mandate"
                }
            },
            {
                "$unwind": {
                    "path": "$mandate",
                    "preserveNullAndEmptyArrays": True
                }
            },
            {
                "$addFields": {
                    "savedWithSAYI": {
                        "$cond": {
                            "if": {"$or": [
                                {"$gt": ["$payment.savedWithSAYI", 0]}
                            ]},
                            "then": True,
                            "else": False
                        }
                    },
                    "activatedMandate": {
                        "$cond": {
                            "if": {"$or": [
                                {"$gt": ["$payment.activatedMandate", 0]}
                            ]},
                            "then": True,
                            "else": False
                        }
                    },
                    "savedWithMandate": {
                        "$cond": {
                            "if": {"$or": [
                                {"$gt": ["$payment.savedWithMandate", 0]}
                            ]},
                            "then": True,
                            "else": False
                        }
                    },
                    "savedManually": {
                        "$cond": {
                        "if": {"$or": [
                                {"$gt": ["$payment.savedManually", 0]}
                            ]},
                            "then": True,
                            "else": False
                        }
                    },
                    "daysFromLastActivity": {
                        "$dateDiff": {
                            "startDate": "$mandate.lastActivity",
                            "endDate": "$$NOW",
                            "unit": "day"
                        }
                    },
                    "daysFromLastPayment": {
                        "$dateDiff": {
                            "startDate": "$payment.lastActivity",
                            "endDate": "$$NOW",
                            "unit": "day"
                        }
                    }
                }
            },
            {
                "$addFields": {
                    "churned": {
                        "$cond": {
                            "if": {"$and": [
                                {"$or": [
                                    {"$gt": ["$payment.savedWithSAYI", 0]},
                                    {"$gt": ["$payment.activatedMandate", 0]},
                                    {"$gt": ["$payment.savedWithMandate", 0]},
                                    {"$gt": ["$payment.savedManually", 0]}
                                ]},
                                {"$and": [
                                    {"$gt": ["$daysFromLastPayment", 7]},
                                    {"$gt": ["$daysFromLastActivity", 7]}
                                ]}
                                
                            ]},
                            "then": True,
                            "else": False
                        }
                    }
                }
            },
            {
                "$match": {
                    "churned": True
                }
            },
            {
                "$project": {
                    "name": 1,
                    "email": 1,
                    "phoneNumber": 1,
                    "payments.count": "$payment.count",
                    "payments.manual_savings": "$payment.savedManually",
                    "payments.cron_savings": "$payment.savedWithMandate",
                    "payments.sayi_savings": "$payment.savedWithSAYI",
                    "payments.mandate_activation": "$payment.activatedMandate",
                    "mandate.count": 1,
                    "mandate.active": "$mandate.activeCount",
                    "mandate.deleted": "$mandate.deletedCount",
                    "mandate.lastActivity": 1,
                    "daysFromLastActivity": 1,
                    "daysFromLastPayment": 1,
                    "payment.lastActivity": 1
                }
            }
        ]

    result = users.aggregate(pipeline)
    storage = list(result) # execute and store query's results

    if storage != []:
        df = pd.DataFrame(storage)

        df[['mandate.count', 'mandate.lastActivity', 'mandate.active', 'mandate.deleted']] = pd.json_normalize(df['mandate'])
        df['payment.lastActivity'] = pd.json_normalize(df['payment'])

        df = df.drop(['mandate', 'payment'], axis=1)

        data = df.to_csv(index=False, encoding='utf-8')
        return data
    else:
        return []
