# %%
import time
import os
import requests
import json
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv  # pip install python-dotenv


def get_50_reports_after_cursor(cur=None):
    # resp after cur # not included
    data = {
        "operationName": "GetReports",
        "variables": {
            "input": {
                "chains": [],
                "scamCategories": [],
                "orderBy": {"field": "CREATED_AT", "direction": "ASC"},
            },
            "first": 50,  # MAX
        },
        "query": "query GetReports($input: ReportsInput, $after: String, $before: String, $last: Float, $first: Float) {\n  reports(\n    input: $input\n    after: $after\n    before: $before\n    last: $last\n    first: $first\n  ) {\n    pageInfo {\n      hasNextPage\n      hasPreviousPage\n      startCursor\n      endCursor\n      __typename\n    }\n    edges {\n      cursor\n      node {\n        ...Report\n        __typename\n      }\n      __typename\n    }\n    count\n    totalCount\n    __typename\n  }\n}\n\nfragment Report on Report {\n  id\n  isPrivate\n  ...ReportPreviewDetails\n  ...ReportAccusedScammers\n  ...ReportAuthor\n  ...ReportAddresses\n  ...ReportEvidences\n  ...ReportCompromiseIndicators\n  ...ReportTokenIDs\n  ...ReportTransactionHashes\n  __typename\n}\n\nfragment ReportPreviewDetails on Report {\n  createdAt\n  scamCategory\n  categoryDescription\n  biDirectionalVoteCount\n  viewerDidVote\n  description\n  lexicalSerializedDescription\n  commentsCount\n  source\n  checked\n  __typename\n}\n\nfragment ReportAccusedScammers on Report {\n  accusedScammers {\n    id\n    info {\n      id\n      contact\n      type\n      __typename\n    }\n    __typename\n  }\n  __typename\n}\n\nfragment ReportAuthor on Report {\n  reportedBy {\n    id\n    username\n    trusted\n    __typename\n  }\n  __typename\n}\n\nfragment ReportAddresses on Report {\n  addresses {\n    id\n    address\n    chain\n    domain\n    label\n    __typename\n  }\n  __typename\n}\n\nfragment ReportEvidences on Report {\n  evidences {\n    id\n    description\n    photo {\n      id\n      name\n      description\n      url\n      __typename\n    }\n    __typename\n  }\n  __typename\n}\n\nfragment ReportCompromiseIndicators on Report {\n  compromiseIndicators {\n    id\n    type\n    value\n    __typename\n  }\n  __typename\n}\n\nfragment ReportTokenIDs on Report {\n  tokens {\n    id\n    tokenId\n    __typename\n  }\n  __typename\n}\n\nfragment ReportTransactionHashes on Report {\n  transactionHashes {\n    id\n    hash\n    chain\n    label\n    __typename\n  }\n  __typename\n}\n",
    }

    if cur is not None:
        data["variables"]["after"] = cur

    resp = None
    while True:
        try:
            resp = requests.post(
                "https://www.chainabuse.com/api/graphql-proxy", json=data
            )
            ret = resp.json()

            return ret
        except Exception as e:
            print(f"from {cur}, result is not json")
            time.sleep(10)


def has_next(result):
    if result["data"]["reports"].get("pageInfo") is None:
        return False

    return result["data"]["reports"]["pageInfo"]["hasNextPage"] == True


# %%
if __name__ == "__main__":
    load_dotenv()
    client = MongoClient(os.getenv("DIRTY_MONGO_URI"))
    db = client["labels"]
    coll = db["chainAbuse"]

    cur = None
    chainabuse_result = None
    total_upserted = 0
    total_modified = 0
    total_matched = 0
    while True:
        chainabuse_result = get_50_reports_after_cursor(cur)
        if chainabuse_result.get("data") is None:  # when TOO MANY REQUESTS
            print(f"from {cur}, chainabuse_result has no data")
            with open(f"error_{time.time()}.log", "w") as f:
                json.dump(chainabuse_result, f)
            time.sleep(3600)
            continue

        if chainabuse_result["data"].get("reports") is None:
            print(f"from {cur}, chainabuse_result has no reports")
            with open(f"error_{time.time()}.log", "w") as f:
                json.dump(chainabuse_result, f)
            time.sleep(600)
            continue

        bulk = []
        if len(chainabuse_result["data"]["reports"]["edges"]) == 0:
            print(f"no more reports, cur {cur}")
            time.sleep(3600)

        for report in chainabuse_result["data"]["reports"]["edges"]:
            bulk.append(
                UpdateOne(
                    filter={"node.id": report["node"]["id"]},
                    update={"$set": report},
                    upsert=True,
                )
            )

        mongo_result = coll.bulk_write(requests=bulk)
        total_upserted += mongo_result.upserted_count
        total_modified += mongo_result.modified_count
        total_matched += mongo_result.matched_count

        print(
            "done: upserted {}/{}, modified {}/{}, matched: {}/{}".format(
                mongo_result.upserted_count,
                total_upserted,
                mongo_result.modified_count,
                total_modified,
                mongo_result.matched_count,
                total_matched,
            )
        )

        next_cur = chainabuse_result["data"]["reports"]["pageInfo"].get("endCursor")
        if next_cur is not None:
            cur = next_cur

        if has_next(chainabuse_result):
            continue
        else:
            print(f"updated to latest, cur: {cur}")
            time.sleep(3600)
