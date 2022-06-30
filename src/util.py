
import httpx
from environs import Env

from datetime import datetime

from numpy import number

env = Env()
env.read_env()

GH_TOKEN = env.str("GH_PAT")

OWNER = env.str("OWNER")
REPOS = env.list("REPOS")


def call_issues_endpoint(owner, repo, state="all"):
    headers = {
        "Authorization": f"token {GH_TOKEN}"
    }
    query_params = {
        "state": state
    }

    resp = httpx.get(f"https://api.github.com/repos/{owner}/{repo}/issues", headers=headers, params=query_params)
    data = resp.json()

    if data == []:
        return data
    
    if resp.status_code not in (200, 304):
        # log an error here
        return []
    
    issue_count = data[0]['number'] # the number of all prs created is the number of the last one
    number_of_pages = (issue_count // 100) + 1

    if number_of_pages > 1:
        accumulator = data
        for page in range(2, number_of_pages + 1):
            query_params['page'] = page
            resp = httpx.get(f"https://api.github.com/repos/{owner}/{repo}/pulls", headers=headers, params=query_params)
            data = resp.json()
            accumulator.extend(data)
        data = accumulator
    
    for item in data:
        item['user'] = item['user']['login']
        item['repo'] = f"{owner}/{repo}"
        item['assignee'] = item['assignee']['login'] if item['assignee'] != None else None
        item['assignees'] = ",".join([_['login'] for _ in item['assignees']])
        item['labels'] = ",".join([_['name'] for _ in item['labels']])
        item['created_at'] = datetime.strptime(item['created_at'], '%Y-%m-%dT%H:%M:%SZ').strftime('%-m/%-d/%Y %H:%M:%S')
        item['updated_at'] = datetime.strptime(item['updated_at'], '%Y-%m-%dT%H:%M:%SZ').strftime('%-m/%-d/%Y %H:%M:%S')
        item['closed_at'] = datetime.strptime(item['closed_at'], '%Y-%m-%dT%H:%M:%SZ').strftime('%-m/%-d/%Y %H:%M:%S') if item['closed_at'] != None else None
        item['is_pr'] = True if item.get('pull_request', None) != None else False
        item['pr_number'] = item['number'] if item['is_pr'] else None

    return data

def call_pulls_endpoint(owner, repo, state="all"):
    headers = {
        "Authorization": f"token {GH_TOKEN}"
    }
    query_params = {
        "state": state,
        "per_page": 100,
        "page": 1
    }
    
    resp = httpx.get(f"https://api.github.com/repos/{owner}/{repo}/pulls", headers=headers, params=query_params)
    data = resp.json()

    if data == []:
        return data
    
    if resp.status_code not in (200, 304):
        # log an error here
        return []
    
    pull_count = data[0]['number'] # the number of all prs created is the number of the last one
    number_of_pages = (pull_count // 100) + 1

    if number_of_pages > 1:
        accumulator = data
        for page in range(2, number_of_pages + 1):
            query_params['page'] = page
            resp = httpx.get(f"https://api.github.com/repos/{owner}/{repo}/pulls", headers=headers, params=query_params)
            data = resp.json()
            accumulator.extend(data)
        data = accumulator

    for item in data:
        item['user'] = item['user']['login']
        item['repo'] = f"{owner}/{repo}"
        item['assignee'] = item['assignee']['login'] if item['assignee'] != None else None
        item['assignees'] = ",".join([_['login'] for _ in item['assignees']])
        item['labels'] = ",".join([_['name'] for _ in item['labels']])
        item['created_at'] = datetime.strptime(item['created_at'], '%Y-%m-%dT%H:%M:%SZ').strftime('%-m/%-d/%Y %H:%M:%S')
        item['updated_at'] = datetime.strptime(item['updated_at'], '%Y-%m-%dT%H:%M:%SZ').strftime('%-m/%-d/%Y %H:%M:%S')
        item['closed_at'] = datetime.strptime(item['closed_at'], '%Y-%m-%dT%H:%M:%SZ').strftime('%-m/%-d/%Y %H:%M:%S') if item['closed_at'] != None else None
        item['merged_at'] = datetime.strptime(item['merged_at'], '%Y-%m-%dT%H:%M:%SZ').strftime('%-m/%-d/%Y %H:%M:%S') if item['merged_at'] != None else None

    return data
