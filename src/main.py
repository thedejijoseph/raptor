
import pandas as pd
from fastapi import FastAPI
from pyparsing import col


from util import REPOS, OWNER
from util import call_issues_endpoint, call_pulls_endpoint

app = FastAPI()

@app.get("/")
async def read_root():
    return {"message": "Raptor Server v0.1"}

@app.get("/issues")
async def fetch_issues():
    accumulator = []
    column_filter = [
        "url", "id", "number", "state", "locked", "title",
        "body", "created_at", "updated_at", "closed_at",
        "assignee", "assignees", "labels", "milestone", "repo", "user"
    ]

    for repo in REPOS:
        issues = call_issues_endpoint(OWNER, repo)
        accumulator.extend(issues)
    
    df = pd.DataFrame(accumulator)
    csv = df.to_csv(index=False, encoding='utf-8', columns=column_filter)

    return csv

@app.get("/pulls")
async def fetch_pulls():
    accumulator = []
    column_filter = [
        "url", "id", "number", "state", "locked", "title",
        "body", "created_at", "updated_at", "closed_at", "merged_at",
        "assignee", "assignees", "labels", "milestone", "repo", "user"
    ]

    for repo in REPOS:
        print(repo)
        pull_requests = call_pulls_endpoint(OWNER, repo)
        accumulator.extend(pull_requests)
    
    df = pd.DataFrame(accumulator)
    csv = df.to_csv(index=False, encoding='utf-8', columns=column_filter)
    
    return csv
