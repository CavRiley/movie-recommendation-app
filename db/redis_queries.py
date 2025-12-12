from redis import Redis
import json
import random
from typing import List, Dict, Optional


def get_random_movie(redis: Redis) -> Optional[Dict]:
    try:
        # get all movie keys
        # would hardcode this but we don't so it's dynamic for adding movies
        movie_keys = redis.keys("movie:*")
        if not movie_keys:
            return None
        
        # pick a random movie key
        random_key = random.choice(movie_keys)
        movie_data = redis.hgetall(random_key)
        
        if not movie_data:
            return None
        
        # extract movie ID from key (format: "movie:123")
        movie_id = int(random_key.split(':')[-1])
        
        return {
            'movieId': movie_id,
            'title': movie_data.get('title', ''),
            'genres': movie_data.get('genre', ''),
            'avg_rating': float(movie_data.get('avg_rating', 0.0))
        }
    except Exception as e:
        print(f"Error getting random movie: {e}")
        return None


def search_movies(redis: Redis, term: str):
    try:
        res = redis.ft("movies_index").search(term)
        return res
    except Exception as e:
        print(f"Search error: {e}")
        return None

# caching the users ratings in redis for 30 min
def cache_user_ratings(redis: Redis, user_id: int, ratings: List[Dict], expire_seconds: int = 1800):
    cache_key = f"user_ratings:{user_id}"
    
    try:
        # convert ratings to JSON string
        ratings_json = json.dumps(ratings)
        
        # store ratingswith expiration
        redis.setex(cache_key, expire_seconds, ratings_json)
        
        return True
    except Exception as e:
        print(f"Error caching user ratings: {e}")
        return False


# grab cached user ratings from redis
def get_cached_user_ratings(redis: Redis, user_id: int) -> Optional[List[Dict]]:
    cache_key = f"user_ratings:{user_id}"
    
    try:
        cached_data = redis.get(cache_key)
        
        if cached_data is None:
            return None
        
        # parse JSON string back to list
        ratings = json.loads(cached_data)
        return ratings
    except Exception as e:
        print(f"Error retrieving cached ratings: {e}")
        return None

# update the average rating for a movie in redis
def update_movie_avg_rating(redis: Redis, movie_id: int, new_avg_rating: float):
    movie_key = f"movie:{movie_id}"
    
    try:
        redis.hset(movie_key, 'avg_rating', new_avg_rating)
        return True
    except Exception as e:
        print(f"Error updating movie rating: {e}")
        return False