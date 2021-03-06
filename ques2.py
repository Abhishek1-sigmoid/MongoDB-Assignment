from bson import ObjectId
from ques1 import *
import json

db = get_database()
def insert_comments():
    collection_name = db['comments']
    collection_data = []
    f = open('sample_mflix/comments.json', 'r')
    lines = f.readlines()
    for line in lines:
        comment = json.loads(line)
        comment['_id'] = ObjectId(comment['_id']['$oid'])
        comment['movie_id'] = ObjectId(comment['movie_id']['$oid'])
        comment['date'] = comment['date']['$date']['$numberLong']
        collection_data.append(comment)
    if isinstance(collection_data, list):
        collection_name.insert_many(collection_data)
    else:
        collection_name.insert_one(collection_data)


def insert_movies():
    collection_name = db['movies']
    collection_data = []
    with open('sample_mflix/movies.json') as f:
        for i in f:
            if i:
                data = json.loads(i)
                data["_id"] = ObjectId(data["_id"]["$oid"])
                collection_data.append(data)
    collection_name.insert_many(collection_data)

def insert_sessions():
    collection_name = db['sessions']
    collection_data = []
    with open('sample_mflix/sessions.json') as f:
        for json_obj in f:
            data = json.loads(json_obj)
            data["_id"] = ObjectId(data["_id"]["$oid"])
            collection_data.append(data)
    collection_name.insert_many(collection_data)

def insert_theaters():
    collection_name = db['theaters']
    collection_data = []
    with open('sample_mflix/theaters.json') as f:
        for i in f:
            if i:
                data = json.loads(i)
                data["_id"] = ObjectId(data["_id"]["$oid"])
                collection_data.append(data)
    collection_name.insert_many(collection_data)

def insert_users():
    collection_name = db['users']
    collection_data = []
    with open('sample_mflix/users.json') as f:
        for i in f:
            if i:
                data = json.loads(i)
                data["_id"] = ObjectId(data["_id"]["$oid"])
                collection_data.append(data)
    collection_name.insert_many(collection_data)


insert_comments()
insert_movies()
insert_theaters()
insert_sessions()
insert_users()
