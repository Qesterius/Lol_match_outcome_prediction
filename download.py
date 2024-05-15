import requests
import os
import pymongo
import pandas as pd
from ratelimit import limits, sleep_and_retry, RateLimitException

client = pymongo.MongoClient("mongodb://root:example@localhost:27017/")

db = client.lol

db.matches.create_index(("metadata.matchId", pymongo.HASHED))
db.timelines.create_index(("metadata.matchId", pymongo.HASHED))

headers = {
    "X-Riot-Token": os.getenv('RIOT_API_KEY')
}

@sleep_and_retry
@limits(calls=20, period=1)
@limits(calls=100, period=120)
def fetch(url: str) -> requests.Response:
    resp = requests.get(url, headers=headers)
    if resp.status_code == 429:
        after = resp.headers.get("Retry-After")
        raise RateLimitException("Rate-Limit-Exceeded", int(after))
    return resp


data = pd.read_csv("match_data_v5.csv")

for match_id in data.matchId:
    match_id = match_id[:-1]
    if db.matches.find_one({"metadata.matchId":  match_id}, {"metadata.matchId": 1}) is None:
        print(f"feching match {match_id}")
        resp = fetch(f"https://europe.api.riotgames.com/lol/match/v5/matches/{match_id}")
        db.matches.insert_one(resp.json())
    if db.timelines.find_one({"metadata.matchId":  match_id}, {"metadata.matchId": 1}) is None:
        print(f"feching timeline {match_id}")
        resp = fetch(f"https://europe.api.riotgames.com/lol/match/v5/matches/{match_id}/timeline")
        db.timelines.insert_one(resp.json())