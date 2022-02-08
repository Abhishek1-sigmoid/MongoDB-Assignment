from ques1 import *
import pandas as pd


# ques 4.b.i.1
def ques1_1(n):
    movie = movies.aggregate([
        {'$project': {'title': '$title', 'rating': '$imdb.rating'}},
        {'$match': {'rating': {'$exists': True, '$ne': ''}}},
        {'$group': {'_id': {'rating': '$rating', 'title': '$title'}}},
        {'$sort': {'_id.rating': -1}},
        {'$limit': n}
    ])
    data = dict()
    data['Movie'] = []
    data['Rating'] = []
    for i in movie:
        data['Movie'].append(i['_id']['title'])
        data['Rating'].append(i['_id']['rating'])
    df = pd.DataFrame(data)
    return df


# ques 4.b.i.2
def ques1_2(n, year):
    movie = movies.aggregate(
        [
            {"$match": {"year": year,"imdb.rating" :{'$exists': True, '$ne': ''}}},
            {"$project": {"_id": 0, "title": 1, "imdb.rating": 1, "year": 1}},
            {"$sort": {"imdb.rating": -1}},
            {"$limit": n}
        ]
    )
    data = dict()
    data['Title'] = []
    data['Rating'] = []
    data['Year'] = []
    for i in movie:
        data['Title'].append(i['title'])
        data['Rating'].append(i['imdb']['rating'])
        data['Year'].append(i['year'])

    df = pd.DataFrame(data)
    return df


# ques 4.b.i.3
def ques1_3(n, threshold):
    movie = movies.aggregate(
        [
            {"$match": {"imdb.votes": {"$gt": threshold}}},
            {"$project": {"_id": 0, "title": 1, "imdb.votes": 1}},
            {"$sort": {"imdb.rating": -1}},
            {"$limit": n}
        ]
    )
    data = dict()
    data['Movie'] = []
    data['Vote'] = []
    for i in movie:
        data['Movie'].append(i['title'])
        data['Vote'].append(i['imdb']['votes'])
    df = pd.DataFrame(data)
    return df


# ques 4.b.i.4
def ques1_4(n, pattern):
    movie = movies.aggregate(
        [
            {"$match": {"title": {"$regex": pattern}}},
            {"$project": {"_id": 0, "title": 1, "tomatoes.viewer.rating": 1}},
            {"$sort": {"tomatoes.viewer.rating": -1}},
            {"$limit": n}
        ]
    )
    title = []
    print(f"Top {n} title matching a given pattern are: ")
    for i in movie:
        title.append(i['title'])
    return title


# ques 4.b.ii.1
def ques2_1(n):
    movie = movies.aggregate(
        [
            {"$unwind": "$directors"},
            {"$group": {"_id": {"director": "$directors"}, "total_movies": {"$sum": 1}}},
            {"$sort": {"total_movies": -1}},
            {"$limit": n}
        ]
    )
    data = dict()
    data['Actor'] = []
    data['Total_movies'] = []
    for i in movie:
        data['Actor'].append(i['_id']['director'])
        data['Total_movies'].append(i['total_movies'])
    df = pd.DataFrame(data)
    return df


# ques 4.b.ii.2
def ques2_2(n, year):
    movie = movies.aggregate(
        [
            {"$unwind": "$directors"},
            {"$group": {"_id": {"directors": "$directors", "year": "$year"}, "no_of_movies": {"$sum": 1}}},
            {"$sort": {"no_of_movies": -1}},
            {"$match": {"_id.year": year}},
            {"$project": {"_id.directors": 1, "no_of_movies": 1, "_id.year": 1}},
            {"$limit": n}
        ]
    )
    data = dict()
    data['Directors'] = []
    data['Total_movies'] = []
    data['Year'] = []
    for i in movie:
        data['Directors'].append(i['_id']['year'])
        data['Total_movies'].append(i['_id']['directors'])
        data['Year'].append(i['no_of_movies'])
    df = pd.DataFrame(data)
    return df


# ques 4.b.ii.3
def ques2_3(n, genre):
    movie = movies.aggregate(
        [
            {"$unwind": "$directors"},
            {"$unwind": "$genres"},
            {"$group": {"_id": {"directors": "$directors", "genres": "$genres"}, "no_of_movies": {"$sum": 1}}},
            {"$sort": {"no_of_movies": -1}},
            {"$match": {"_id.genres": genre}},
            {"$limit": n}
        ]
    )
    data = dict()
    data['Directors'] = []
    data['Total_movies'] = []
    data['Genre'] = []
    for i in movie:
        data['Directors'].append(i['_id']['directors'])
        data['Total_movies'].append(i['no_of_movies'])
        data['Genre'].append(genre)
    df = pd.DataFrame(data)
    return df


# ques 4.b.iii.1
def ques3_1(n):
    movie = movies.aggregate([
        {"$unwind": "$cast"},
        {"$group": {"_id": {"cast": "$cast"}, "no_of_films": {"$sum": 1}}},
        {"$sort": {"no_of_films": -1}},
        {"$limit": n}
    ])
    data = dict()
    data['Actor'] = []
    data['Total_movies'] = []
    for i in movie:
        data['Actor'].append(i['_id']['cast'])
        data['Total_movies'].append(i['no_of_films'])
    df = pd.DataFrame(data)
    return df


# ques 4.b.iii.2
def ques3_2(n, year):
    movie = movies.aggregate([
        {"$unwind": "$cast"},
        {"$group": {"_id": {"cast": "$cast", "year": "$year"}, "no_of_movies": {"$sum": 1}}},
        {"$sort": {"no_of_movies": -1}},
        {"$match": {"_id.year": year}},
        {"$project": {"_id.year": 0}},
        {"$limit": n}
    ])
    data = dict()
    data['Actor'] = []
    data['Total_movies'] = []
    data['Year'] = []
    for i in movie:
        data['Actor'].append(year)
        data['Total_movies'].append(i['_id']['cast'])
        data['Year'].append(i['no_of_movies'])
    df = pd.DataFrame(data)
    return df


# ques 4.b.iii.3
def ques3_3(n, genre):
    movie = movies.aggregate(
        [
            {"$unwind": "$cast"},
            {"$unwind": "$genres"},
            {"$group": {"_id": {"cast": "$cast", "genres": "$genres"}, "no_of_movies": {"$sum": 1}}},
            {"$sort": {"no_of_movies": -1}},
            {"$match": {"_id.genres": genre}},
            {"$project": {"_id.genres": 0}},
            {"$limit": n}
        ]
    )
    data = dict()
    data['Actor'] = []
    data['Total_movies'] = []
    data['Genre'] = []
    for i in movie:
        data['Actor'].append(i['_id']['cast'])
        data['Total_movies'].append(i['no_of_movies'])
        data['Genre'].append(genre)
    df = pd.DataFrame(data)
    return df


# ques 4.b.iv
def ques4(n):
    movie = movies.aggregate(
        [
            {"$unwind": "$genres"},
            {"$project": {"rating": "$imdb.rating", "genres": "$genres", "title": "$title"}},
            {'$match': {'rating': {'$exists': True, '$ne': ''}}},
            {"$group": {"_id": {"genres": "$genres", "max_rating": {"$max": "$rating"}, "title": {"first": "$title"}}}},
            {"$sort": {"_id.max_rating": -1}},
            {"$limit": n}
        ]
    )
    data = dict()
    data['Title'] = []
    data['Rating'] = []
    data['Genre'] = []
    for i in movie:
        data['Title'].append(i['_id']['title']['first'])
        data['Rating'].append(i['_id']['max_rating'])
        data['Genre'].append(i['_id']['genres'])
    df = pd.DataFrame(data)
    return df


if __name__ == "__main__":
    n = 5
    print(f"Top {n} Movies with")
    print("The highest IMDB rating:")
    print(ques1_1(n), end='\n\n')
    print("The highest IMDB rating in a given year: ")
    print(ques1_2(n, 2012), end='\n\n')
    print("The highest IMDB rating with number of votes > 1000: ")
    print(ques1_3(n, 1000), end='\n\n')
    print("The title matching a given pattern sorted by highest tomatoes ratings: ")
    print(ques1_4(n, "The"), end='\n\n')
    print(f"Top {n} directors")
    print("Who created the maximum number of movies: ")
    print(ques2_1(n), end='\n\n')
    print("Who created the maximum number of movies in a given year: ")
    print(ques2_2(n, 2012), end='\n\n')
    print("Who created the maximum number of movies for a given genre: ")
    print(ques2_3(n, "Action"), end='\n\n')
    print(f"Top {n} Actors")
    print("Who starred in the maximum number of movies: ")
    print(ques3_1(n), end='\n\n')
    print("Who starred in the maximum number of movies in a given year: ")
    print(ques3_2(n, 2012), end='\n\n')
    print("Who starred in the maximum number of movies for a given genre: ")
    print(ques3_3(n, "Action"), end='\n\n')
    print(f"Top {n} movies for each genre with the highest IMDB rating: ")
    print(ques4(n), end='\n')
