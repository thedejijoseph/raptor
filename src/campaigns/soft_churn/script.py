
import json
from io import StringIO
from typing import Any, List, Dict, Tuple

import pandas as pd

import httpx
from environs import Env
from customerio import CustomerIO, Regions


env = Env()
env.read_env()

HEADERS = {}

CIO_SITE_ID = env('CIO_SITE_ID')
CIO_TRK_API_KEY = env('CIO_TRK_API_KEY')
CIO_APP_API_KEY = env('CIO_APP_API_KEY')

CIO_APP_AUTH = {
    "content-type": "application/json",
    "Authorization": f"Bearer {CIO_APP_API_KEY}"
}
HEADERS.update(CIO_APP_AUTH)

CAMPAIGN_BROADCAST_ID = 21

CIO_TRK_API_US = "https://track.customer.io/api/v1"
CIO_APP_API_US = "https://api.customer.io/v1"

cio = CustomerIO(site_id=CIO_SITE_ID, api_key=CIO_TRK_API_KEY, region=Regions.US)

def make_customers(csv_list:str) -> Dict[str, Dict[str, Any]]:
    """Expected data for each campaign is data in CSV format.
    
    `email` is a reuired column. all others are optional
    """

    if csv_list == "":
        # log: empty dataset
        return {}
    
    try:
        # improves resilience when script opreates unattended to
        # logs are critical for debugging and traceability
        csv_data = StringIO(csv_list)
        df = pd.read_csv(csv_data)
        df = df.drop(columns=['payments'])

        json_data = json.loads(df.to_json(orient='records'))
        customers = {}

        for record in json_data:
            email = record.pop('email')
            customers[email] = record
    except:
        # log errors
        # raise
        return {}
    
    return customers

def upload_customers(customers: Dict[str, Dict[str, Any]]) -> Tuple[bool, Dict[str, bool]]:
    """Upload new customers to customer.io"""
    
    #log: uploading customers
    uploaded = {}
    try:
        for email, attributes in customers.items():
            # log: uploading email
            cio.identify(id=email, **attributes)
            uploaded[email] = True
        return True, uploaded
    except:
        # log and optionally raise
        # raise
        return False, {}

def __create_new_segment(name, description:str = "") -> Tuple[int, Dict[str, Any]]:
    """Create a new manual segment on customer.io"""

    # not in use: cannot add customers to segments via api
    payload = {
        "segment": {
            "name": name,
            "description": description
        }
    }

    endpoint = f"{CIO_APP_API_US}/segments"
    response = httpx.post(endpoint, json=payload, headers=HEADERS)

    if response.status_code == 200:
        return response.status_code, response.json()
    else:
        response_data = {
            "message": "failed to make segment",
            "status_code": response.status_code,
            "data": response.json()
        }
        return response.status_code, response_data

def __segment_users(
        customers: Dict[str, Dict[str, Any]], 
        segment_name:str, 
        segment_descripton: str
    ) -> Tuple[int, Dict[str, Any]]:
    """This function creates a new segment and adds users to it.
    
    If the users did not exist on CIO already, it will add them.
    This function should be called once, and only before a campaign is triggered.

    Segment name is not unique, and Segment ID is not persisted.
    """

    # not in use
    status, segment = __create_new_segment(segment_name, segment_descripton)
    if status != 200:
        # log: failed to create segment, terminating
        return
    
    upload_complete, uploaded_users = upload_customers(customers)

    if not upload_complete:
        # log: failed to upload all users
        return

def prepare_recipients(customers: Dict[str, Dict[str, Any]]) -> list:
    """Upload (update or create) customers and related attributes."""
    
    # log: preparing email list
    upload_complete, uploaded_users = upload_customers(customers)

    if not upload_complete:
        # log: failed to upload all users
        return []
    
    email_list = list(uploaded_users.keys())
    return email_list

def trigger_broadcast(emails: List[str]) -> Tuple[int, Dict[str, Any]]:
    """Trigger a CIO Broadcast"""

    payload = {
        "emails": emails,
    }

    endpoint = f"{CIO_APP_API_US}/campaigns/{CAMPAIGN_BROADCAST_ID}/triggers"
    response = httpx.post(endpoint, json=payload, headers=HEADERS)

    if response.status_code == 200:
        return response.status_code, response.json()
    else:
        response_data = {
            "message": "failed to fire broadcast",
            "status_code": response.status_code,
            "data": response.json()
        }
        return response.status_code, response_data

def handler(csv_list:str) -> Tuple[int, Dict[str, Any]]:
    """Entry point for the campaign's script.
    
    Data is expected in CSV format.
    """
    
    customers = make_customers(csv_list)
    email_list = prepare_recipients(customers)

    status, broadcast = trigger_broadcast(email_list)
    return status, broadcast
