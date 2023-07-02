# %% 
import time
import os
import requests
from pymongo import MongoClient
from dotenv import load_dotenv # pip install python-dotenv

def get_50_reports_after_cursor(cur=None):
    # resp after cur # not included
    data = {
        "operationName": "GetReports",
        "variables": {
            "input": {
                "chains": [],
                "scamCategories": [],
                "orderBy": {
                    "field": "CREATED_AT",
                    "direction": "ASC"
                }
            },
            "first": 50, # MAX
        },
        "query": "query GetReports($input: ReportsInput, $after: String, $before: String, $last: Float, $first: Float) {\n  reports(\n    input: $input\n    after: $after\n    before: $before\n    last: $last\n    first: $first\n  ) {\n    pageInfo {\n      hasNextPage\n      hasPreviousPage\n      startCursor\n      endCursor\n      __typename\n    }\n    edges {\n      cursor\n      node {\n        ...Report\n        __typename\n      }\n      __typename\n    }\n    count\n    totalCount\n    __typename\n  }\n}\n\nfragment Report on Report {\n  id\n  isPrivate\n  ...ReportPreviewDetails\n  ...ReportAccusedScammers\n  ...ReportAuthor\n  ...ReportAddresses\n  ...ReportEvidences\n  ...ReportCompromiseIndicators\n  ...ReportTokenIDs\n  ...ReportTransactionHashes\n  __typename\n}\n\nfragment ReportPreviewDetails on Report {\n  createdAt\n  scamCategory\n  categoryDescription\n  biDirectionalVoteCount\n  viewerDidVote\n  description\n  lexicalSerializedDescription\n  commentsCount\n  source\n  checked\n  __typename\n}\n\nfragment ReportAccusedScammers on Report {\n  accusedScammers {\n    id\n    info {\n      id\n      contact\n      type\n      __typename\n    }\n    __typename\n  }\n  __typename\n}\n\nfragment ReportAuthor on Report {\n  reportedBy {\n    id\n    username\n    trusted\n    __typename\n  }\n  __typename\n}\n\nfragment ReportAddresses on Report {\n  addresses {\n    id\n    address\n    chain\n    domain\n    label\n    __typename\n  }\n  __typename\n}\n\nfragment ReportEvidences on Report {\n  evidences {\n    id\n    description\n    photo {\n      id\n      name\n      description\n      url\n      __typename\n    }\n    __typename\n  }\n  __typename\n}\n\nfragment ReportCompromiseIndicators on Report {\n  compromiseIndicators {\n    id\n    type\n    value\n    __typename\n  }\n  __typename\n}\n\nfragment ReportTokenIDs on Report {\n  tokens {\n    id\n    tokenId\n    __typename\n  }\n  __typename\n}\n\nfragment ReportTransactionHashes on Report {\n  transactionHashes {\n    id\n    hash\n    chain\n    label\n    __typename\n  }\n  __typename\n}\n"
    }

    if cur is not None:
        data["variables"]["after"] = cur

    resp = None
    while True:
        try:
            resp = requests.post("https://www.chainabuse.com/api/graphql-proxy", json=data)
            ret = resp.json()
            if ret.get("data") is None:
                print(ret)
                time.sleep(10)
                continue

            if ret["data"].get("reports") is None:
                print(ret)
                time.sleep(10)
                continue

            return ret
        except Exception as e:
            if resp is not None:
                print(resp.text)
            print(e)
            time.sleep(10)

    

def has_next(result):
    if result["data"]["reports"].get("pageInfo") is None:
        return False
    
    return result["data"]["reports"]["pageInfo"]["hasNextPage"] == True

def upsert_report(coll, report):
    return coll.update_one(filter={"node.id": report["node"]["id"]}, update={"$set": report}, upsert=True)
 

# %%
if __name__ == "__main__":
    load_dotenv()
    client = MongoClient(os.getenv("MONGO_URI"))
    db = client["labels"]
    coll = db["chainAbuse"]

    cur = None
    result = get_50_reports_after_cursor()
    while True:
        if has_next(result):
            time.sleep(2)
            cur = result["data"]["reports"]["pageInfo"]["endCursor"]

            inserted_count = 0
            modified_count = 0
            for report in result["data"]["reports"]["edges"]:
                upsert_report(coll, report)
                inserted_count +=  result.inserted_count 
                modified_count += result.modified_count

            result = get_50_reports_after_cursor(cur)

            print("done: {} inserted, {} updated".format(inserted_count, modified_count))
        else:
            print("updated to latest")
            time.sleep(3600)
