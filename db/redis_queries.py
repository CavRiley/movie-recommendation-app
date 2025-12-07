from redis import Redis


def search_movies(redis: Redis, term: str):

    try:
        res = redis.ft("movies_index").search(term,)
    except Exception as e:
        return []

    return res

