from pymongo import MongoClient
def get_database():
    connection_url = "mongodb+srv://abhishek:Abhishek%40123@cluster0.s4eup.mongodb.net/test"
    client = MongoClient(connection_url)
    return client["sample_mflix"]


database = get_database()
comments = database['comments']
movies = database['movies']
sessions = database['sessions']
theaters = database['theaters']
users = database['users']
