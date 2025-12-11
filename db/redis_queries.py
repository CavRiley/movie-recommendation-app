from redis import Redis
import json
import random
from typing import List, Dict, Optional


def get_random_movie(redis: Redis) -> Optional[Dict]:
    """Get a random movie from Redis"""
    try:
        # Get all movie keys
        movie_keys = redis.keys("movie:*")
        if not movie_keys:
            return None
        
        # Pick a random movie key
        random_key = random.choice(movie_keys)
        movie_data = redis.hgetall(random_key)
        
        if not movie_data:
            return None
        
        # Extract movie ID from key (format: "movie:123")
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
    """Search movies using full-text search index"""
    try:
        res = redis.ft("movies_index").search(term)
        return res
    except Exception as e:
        print(f"Search error: {e}")
        return None


def cache_user_ratings(redis: Redis, user_id: int, ratings: List[Dict], expire_seconds: int = 1800):
    """
    Cache user ratings in Redis with expiration.
    
    Args:
        redis: Redis connection
        user_id: User ID
        ratings: List of rating dictionaries from Neo4j
        expire_seconds: Cache expiration time (default 30 minutes)
    
    Design choice: Using JSON string for simplicity and flexibility.
    The 30-minute TTL balances freshness with cache effectiveness.
    User ratings don't change frequently, so 30 minutes is reasonable.
    """
    cache_key = f"user_ratings:{user_id}"
    
    try:
        # Convert ratings to JSON string
        ratings_json = json.dumps(ratings)
        
        # Store with expiration
        redis.setex(cache_key, expire_seconds, ratings_json)
        
        return True
    except Exception as e:
        print(f"Error caching user ratings: {e}")
        return False


def get_cached_user_ratings(redis: Redis, user_id: int) -> Optional[List[Dict]]:
    """
    Retrieve cached user ratings from Redis.
    
    Returns:
        List of rating dictionaries if found, None if not in cache
    """
    cache_key = f"user_ratings:{user_id}"
    
    try:
        cached_data = redis.get(cache_key)
        
        if cached_data is None:
            return None
        
        # Parse JSON string back to list
        ratings = json.loads(cached_data)
        return ratings
    except Exception as e:
        print(f"Error retrieving cached ratings: {e}")
        return None


def update_movie_avg_rating(redis: Redis, movie_id: int, new_avg_rating: float):
    """Update the average rating for a movie in Redis"""
    movie_key = f"movie:{movie_id}"
    
    try:
        redis.hset(movie_key, 'avg_rating', new_avg_rating)
        return True
    except Exception as e:
        print(f"Error updating movie rating: {e}")
        return False