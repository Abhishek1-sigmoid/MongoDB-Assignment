from ques1 import *


def insert_comment():
    new_comment = {
        "name": "Steve Roger",
        "text": "Captain America is a great character in MCU  ",
    }
    new_comment_id = comments.insert_one(new_comment).inserted_id
    comment = comments.find_one({"_id": new_comment_id})
    return comment


def insert_movie():
    new_movie = {
        "plot": "spider man and the multiverse",
        "genres": ["Science Fiction"],
        "title": "Spider Man"
    }
    new_movie_id = movies.insert_one(new_movie).inserted_id
    movie = movies.find_one({"_id": new_movie_id})
    return movie

def insert_theatre():
    new_theater = {
        "theater_id": 185,
        "location": {
            "address": {
                "city": "Australia"
            }
        }
    }
    new_theater_id = theaters.insert_one(new_theater).inserted_id
    theater = theaters.find_one({"_id": new_theater_id})
    return theater

def insert_user():
    new_user = {
        "name": "James",
        "email": "james@gmail.com",
        "password": "james1234"
    }
    new_user_id = users.insert_one(new_user).inserted_id
    user = users.find_one({"_id": new_user_id})
    return user


if __name__ == "__main__":
    print(insert_comment(), end='\n')
    print(insert_movie(), end='\n')
    print(insert_theatre(), end='\n')
    print(insert_user(), end='\n')
