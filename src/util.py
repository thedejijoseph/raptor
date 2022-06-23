
import httpx
from environs import Env

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

    if resp.status_code not in (200, 304):
        # log an error here
        return []
    
    for item in data:
        item['user'] = item['user']['login']
        item['repo'] = f"{owner}/{repo}"

    return data

def call_pulls_endpoint(owner, repo, state="all"):
    headers = {
        "Authorization": f"token {GH_TOKEN}"
    }
    query_params = {
        "state": state
    }
    
    resp = httpx.get(f"https://api.github.com/repos/{owner}/{repo}/pulls", headers=headers, params=query_params)
    data = resp.json()

    if resp.status_code not in (200, 304):
        # log an error here
        return []

    for item in data:
        item['user'] = item['user']['login']
        item['repo'] = f"{owner}/{repo}"

    return data
