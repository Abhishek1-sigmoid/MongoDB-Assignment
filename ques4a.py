from ques1 import *

# ques 4.a.(i)
def ques4a_1():
    names = comments.aggregate(
        [{
            "$group":
                {"_id": "$name",
                 "number_of_comment": {"$sum": 1}
                 }},
            {"$sort": {"number_of_comment": -1}},
            {"$limit": 10}
        ])
    print("Top 10 users are: ")
    user = []
    for i in names:
        print(i['_id'])


# ques 4.a.(ii)
def ques4a_2():
    comment = comments.aggregate(
        [{
            "$group":
                {"_id": "$movie_id",
                 "movie": {"$sum": 1}
                 }},
            {"$sort": {"movie": -1}},
            {"$limit": 10},
            {'$lookup': {
                'from': 'movies',
                'localField': '_id',
                'foreignField': '_id',
                'as': 'id'
            }},
            {'$unwind': {
                'path': '$id',
                'preserveNullAndEmptyArrays': False
            }},
            {'$project': {
                'id.title': 1
            }}
        ])
    print()
    print("Top 10 Movies are: ")
    movie = []
    for i in comment:
        print(i['id']['title'])


# ques 4.a.(iii)
def ques4a_3(year):
    month = comments.aggregate(
        [
            {"$project": {"month": {"$month": "$date"}, "year": {"$year": "$date"}}},
            {"$match": {"year": year}},
            {"$group": {"_id": {"month": "$month"}, "count": {"$sum": 1}}},
            {"$sort": {"_id.month": 1}}
        ]
    )
    print()
    print("Total comments per month in a given year")
    for i in month:
        print(f"Month = {i['_id']['month']}  Comment = {i['count']}")


ques4a_1()
ques4a_2()
ques4a_3(2012)
