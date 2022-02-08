from ques1 import *
import pandas as pd

# ques 4.c.i
def ques1():
    theater = theaters.aggregate([
        {"$group": {"_id": {"city": "$location.address.city"}, "total_theaters": {"$sum": 1}}},
        {"$sort": {"total_theaters": -1}},
        {"$limit": 10},
        {"$project": {"city": "$_id.city", "_id": 0, "total_theaters": 1}}
    ])
    data = dict()
    data['City'] = []
    data['Count Theaters'] = []
    for i in theater:
        data['City'].append(i['city'])
        data['Count Theaters'].append(i['total_theaters'])
    df = pd.DataFrame(data)
    return df


# ques 4.c.ii
def ques2(lat, lan):
    theater = theaters.aggregate([
        {
            "$geoNear": {
                "near": {"type": "Point", "coordinates": [lat, lan]},
                "key": "location.geo",
                "distanceField": "dist.calculated",
                "spherical": True
            }
        },
        {"$project": {"theaterId": 1, "city": "$location.address.city", "distance": "$dist.calculated"}},
        {"$limit": 10}
    ])
    data = dict()
    data['City'] = []
    data['Distance'] = []
    data['Theater Id'] = []
    for i in theater:
        data['Theater Id'].append(i['theaterId'])
        data['City'].append(i['city'])
        data['Distance'].append(i['distance'])

    df = pd.DataFrame(data)
    return df


print("Top 10 cities with maximum numbers of theaters")
print(ques1(), end='\n\n')
print("top 10 theaters nearby given coordinates")
print(ques2(-73.9667, 40.78))
