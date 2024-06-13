from collections import defaultdict
import json

import pandas as pd
import pymongo
from tqdm import tqdm



buckets = [0.2, 0.4, 0.6, 0.8, 1]

ALL_STATS = [
  "BLUE_BUILDING_KILL_INHIBITOR_BUILDING",
  "BLUE_BUILDING_KILL_TOWER_BUILDING",
  "BLUE_CHAMPION_KILL",
  "BLUE_FIRST_BLOOD",
  "BLUE_MONSTER_KILL_MST_AIR_DRAGON",
  "BLUE_MONSTER_KILL_MST_CHEMTECH_DRAGON",
  "BLUE_MONSTER_KILL_MST_EARTH_DRAGON",
  "BLUE_MONSTER_KILL_MST_ELDER_DRAGON",
  "BLUE_MONSTER_KILL_MST_FIRE_DRAGON",
  "BLUE_MONSTER_KILL_MST_HEXTECH_DRAGON",
  "BLUE_MONSTER_KILL_MST_WATER_DRAGON",
  "BLUE_MONSTER_KILL_MT_BARON_NASHOR",
  "BLUE_MONSTER_KILL_MT_DRAGON",
  "BLUE_MONSTER_KILL_MT_HORDE",
  "BLUE_MONSTER_KILL_MT_RIFTHERALD",
  "RED_BUILDING_KILL_INHIBITOR_BUILDING",
  "RED_BUILDING_KILL_TOWER_BUILDING",
  "RED_CHAMPION_KILL",
  "RED_FIRST_BLOOD",
  "RED_MONSTER_KILL_MST_AIR_DRAGON",
  "RED_MONSTER_KILL_MST_CHEMTECH_DRAGON",
  "RED_MONSTER_KILL_MST_EARTH_DRAGON",
  "RED_MONSTER_KILL_MST_ELDER_DRAGON",
  "RED_MONSTER_KILL_MST_FIRE_DRAGON",
  "RED_MONSTER_KILL_MST_HEXTECH_DRAGON",
  "RED_MONSTER_KILL_MST_WATER_DRAGON",
  "RED_MONSTER_KILL_MT_BARON_NASHOR",
  "RED_MONSTER_KILL_MT_DRAGON",
  "RED_MONSTER_KILL_MT_HORDE",
  "RED_MONSTER_KILL_MT_RIFTHERALD"
]

client = pymongo.MongoClient("mongodb://root:example@localhost:27017/admin")

db = client.lol

matches_events = db.timelines.aggregate([
    {
        "$project": {
            "matchId": "$metadata.matchId",
            "events": {
                "$reduce": {
                    "input": "$info.frames",
                    "initialValue": [],
                    "in": { "$concatArrays": ["$$value", "$$this.events"]}
                } 
            }
        }
    }
])

def participantIdToColor(pid):
    return "BLUE" if pid < 6 else "RED"

total_docs = db.timelines.count_documents({})


match_stats = []
with tqdm(total=total_docs, desc="Processing documents") as pbar:

    for match_events in matches_events:
        stats = defaultdict(list)

        gameEnd = None

        for event in match_events["events"]:
            timestamp = event["timestamp"]
            match event["type"]:
                case "CHAMPION_KILL":
                    color = participantIdToColor(event["killerId"])
                    stats[f"{color}_CHAMPION_KILL"].append(timestamp)
                case "CHAMPION_SPECIAL_KILL" if event["killType"] == "KILL_FIRST_BLOOD":
                    color = participantIdToColor(event["killerId"])
                    stats[f"{color}_FIRST_BLOOD"].append(timestamp)
                case "ELITE_MONSTER_KILL":
                    color = participantIdToColor(event["killerId"])
                    monsterType = event["monsterType"]
                    monsterSubType = event.get("monsterSubType")
                    stats[f"{color}_MONSTER_KILL_MT_{monsterType}"].append(timestamp)
                    if monsterSubType is not None:
                        stats[f"{color}_MONSTER_KILL_MST_{monsterSubType}"].append(timestamp)
                case "BUILDING_KILL":
                    color = participantIdToColor(event["killerId"])
                    buildingType = event["buildingType"]
                    stats[f"{color}_BUILDING_KILL_{buildingType}"].append(timestamp)
                case "GAME_END":
                    gameEnd = timestamp
                case _:
                    pass
        
        matchBuckets = [dict() for _ in range(len(buckets))]
        for bucket, threshold in zip(matchBuckets, buckets):
            bucket["THRESHOLD"] = threshold
            bucket["matchId"] = match_events["matchId"]
            for stat_name in ALL_STATS:
                bucket[stat_name] = 0
        for stat_name, timestamps in stats.items():
            for timestamp in timestamps:
                for bucket, bucket_threshold in zip(matchBuckets, buckets):
                    if timestamp < bucket_threshold * gameEnd:
                        bucket[stat_name] += 1
        match_stats.extend(matchBuckets)
        pbar.update()

df = pd.DataFrame(match_stats)
df.to_csv('data_csv/eventdata.csv', index=False)

    

    

# events = db.timelines.aggregate([
#     {
#         "$project": {
#             "types": "$info.frames.events.type"
#         }
#     },
#     { "$unwind": "$types" },
#     { "$unwind": "$types" },
#     { 
#         "$group": {
#             "_id": "$types"
#         }
#     }
# ])