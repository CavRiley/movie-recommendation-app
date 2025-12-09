from neo4j import GraphDatabase
from typing import List, Dict, Optional


class MovieRecommender:
    """
    Hybrid recommendation system using Neo4j graph database.
    Combines collaborative filtering and content-based filtering.
    """
    
    def __init__(self, uri="bolt://localhost:7687", auth=('neo4j', 'password')):
        self.driver = GraphDatabase.driver(uri, auth=auth)
    
    def close(self):
        self.driver.close()
    
    def get_or_create_user(self, user_id: int, name: Optional[str] = None) -> Dict:
        """Get existing user or create new one with name"""
        with self.driver.session() as session:
            # Check if user exists
            result = session.run("""
                MATCH (u:User {userId: $userId})
                RETURN u.userId as userId, u.name as name
            """, userId=user_id)
            
            record = result.single()
            
            if record:
                # User exists - update name if provided
                if name:
                    session.run("""
                        MATCH (u:User {userId: $userId})
                        SET u.name = $name
                    """, userId=user_id, name=name)
                    return {
                        'userId': record['userId'],
                        'name': name,
                        'exists': True
                    }
                else:
                    return {
                        'userId': record['userId'],
                        'name': record['name'],
                        'exists': True
                    }
            else:
                # Create new user
                if name:
                    session.run("""
                        CREATE (u:User {userId: $userId, name: $name})
                    """, userId=user_id, name=name)
                    return {'userId': user_id, 'name': name, 'exists': False}
                else:
                    session.run("""
                        CREATE (u:User {userId: $userId})
                    """, userId=user_id)
                    return {'userId': user_id, 'exists': False}
    
    def get_user_ratings(self, user_id: int) -> List[Dict]:
        """Get all movies rated by a user"""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (u:User {userId: $userId})-[r:RATED]->(m:Movie)
                RETURN m.movieId as movieId, 
                       m.title as title, 
                       m.full_title as full_title,
                       r.rating as rating,
                       r.timestamp as timestamp
                ORDER BY r.timestamp DESC
            """, userId=user_id)
            
            return [dict(record) for record in result]
    
    def add_rating(self, user_id: int, movie_id: int, rating: float) -> bool:
        """Add or update a user's rating for a movie"""
        with self.driver.session() as session:
            try:
                # Check if rating exists and update or create
                session.run("""
                    MATCH (u:User {userId: $userId})
                    MATCH (m:Movie {movieId: $movieId})
                    MERGE (u)-[r:RATED]->(m)
                    SET r.rating = $rating,
                        r.timestamp = timestamp()
                """, userId=user_id, movieId=movie_id, rating=rating)
                
                # Update movie average rating
                session.run("""
                    MATCH (m:Movie {movieId: $movieId})<-[r:RATED]-()
                    WITH m, avg(r.rating) as avgRating, count(r) as ratingCount
                    SET m.avgRating = avgRating,
                        m.ratingCount = ratingCount
                """, movieId=movie_id)
                
                return True
            except Exception as e:
                print(f"Error adding rating: {e}")
                return False
    
    def get_collaborative_recommendations(self, user_id: int, limit: int = 5) -> List[Dict]:
        """
        Collaborative filtering: Find similar users and recommend movies they liked.
        
        Strategy:
        1. Find users who rated similar movies with similar ratings
        2. Get movies those similar users rated highly
        3. Filter out movies the target user has already rated
        4. Rank by weighted score based on similarity and rating
        """
        with self.driver.session() as session:
            result = session.run("""
                // Find similar users based on common highly-rated movies
                MATCH (target:User {userId: $userId})-[r1:RATED]->(m:Movie)<-[r2:RATED]-(other:User)
                WHERE r1.rating >= 3.5 AND r2.rating >= 3.5 AND target <> other
                
                // Calculate similarity score (number of commonly liked movies)
                WITH other, count(DISTINCT m) as commonMovies, 
                     sum(abs(r1.rating - r2.rating)) as ratingDiff
                WHERE commonMovies >= 3
                
                // Calculate similarity (more common movies, less rating difference = more similar)
                WITH other, commonMovies, 
                     (commonMovies * 1.0) / (1.0 + ratingDiff) as similarity
                ORDER BY similarity DESC
                LIMIT 20
                
                // Get highly-rated movies from similar users that target hasn't seen
                MATCH (other)-[r:RATED]->(rec:Movie)
                WHERE r.rating >= 3.5
                  AND NOT EXISTS {
                      MATCH (target:User {userId: $userId})-[:RATED]->(rec)
                  }
                
                // Aggregate and rank recommendations
                WITH rec, 
                     sum(similarity * r.rating) as score,
                     avg(r.rating) as avgRatingBySimilarUsers,
                     count(DISTINCT other) as recommendedBy,
                     rec.avgRating as overallAvgRating
                
                RETURN rec.movieId as movieId,
                       rec.title as title,
                       rec.full_title as full_title,
                       rec.year as year,
                       overallAvgRating,
                       avgRatingBySimilarUsers,
                       recommendedBy,
                       score
                ORDER BY score DESC, overallAvgRating DESC
                LIMIT $limit
            """, userId=user_id, limit=limit)
            
            return [dict(record) for record in result]
    
    def get_content_based_recommendations(self, user_id: int, limit: int = 5) -> List[Dict]:
        """
        Content-based filtering: Recommend movies similar to those the user liked.
        
        Strategy:
        1. Find genres the user likes based on their ratings
        2. Recommend highly-rated movies in those genres
        3. Filter out movies already rated
        """
        with self.driver.session() as session:
            result = session.run("""
                // Find genres the user likes
                MATCH (u:User {userId: $userId})-[r:RATED]->(m:Movie)-[:HAS_GENRE]->(g:Genre)
                WHERE r.rating >= 3.5
                
                WITH g, avg(r.rating) as avgUserRating, count(m) as genreCount
                ORDER BY avgUserRating DESC, genreCount DESC
                LIMIT 5
                
                // Find highly-rated movies in those genres that user hasn't seen
                MATCH (g)<-[:HAS_GENRE]-(rec:Movie)
                WHERE rec.avgRating >= 3.5
                  AND rec.ratingCount >= 10
                  AND NOT EXISTS {
                      MATCH (u:User {userId: $userId})-[:RATED]->(rec)
                  }
                
                WITH DISTINCT rec, 
                     collect(DISTINCT g.name) as matchedGenres,
                     rec.avgRating * rec.ratingCount as popularityScore
                
                RETURN rec.movieId as movieId,
                       rec.title as title,
                       rec.full_title as full_title,
                       rec.year as year,
                       rec.avgRating as avgRating,
                       rec.ratingCount as ratingCount,
                       matchedGenres,
                       popularityScore
                ORDER BY popularityScore DESC
                LIMIT $limit
            """, userId=user_id, limit=limit)
            
            return [dict(record) for record in result]
    
    def get_hybrid_recommendations(self, user_id: int, limit: int = 5) -> List[Dict]:
        """
        Hybrid approach: Combine collaborative and content-based recommendations.
        
        This provides a balanced set of recommendations that considers both
        what similar users like and what aligns with the user's preferences.
        """
        # Get more recommendations from each method
        collab_recs = self.get_collaborative_recommendations(user_id, limit=limit * 2)
        content_recs = self.get_content_based_recommendations(user_id, limit=limit * 2)
        
        # Create a combined ranking
        movie_scores = {}
        
        # Add collaborative filtering scores (weighted higher for users with more ratings)
        for i, rec in enumerate(collab_recs):
            movie_id = rec['movieId']
            # Higher position = higher score
            collab_score = (len(collab_recs) - i) * 2.0
            movie_scores[movie_id] = {
                'movie': rec,
                'score': collab_score,
                'source': 'collaborative'
            }
        
        # Add content-based scores
        for i, rec in enumerate(content_recs):
            movie_id = rec['movieId']
            content_score = (len(content_recs) - i) * 1.5
            
            if movie_id in movie_scores:
                # Movie appears in both - boost its score
                movie_scores[movie_id]['score'] += content_score
                movie_scores[movie_id]['source'] = 'hybrid'
            else:
                movie_scores[movie_id] = {
                    'movie': rec,
                    'score': content_score,
                    'source': 'content'
                }
        
        # Sort by combined score and return top N
        sorted_recs = sorted(
            movie_scores.values(),
            key=lambda x: x['score'],
            reverse=True
        )[:limit]
        
        # Format the results
        results = []
        for item in sorted_recs:
            movie = item['movie']
            results.append({
                'movieId': movie['movieId'],
                'title': movie['title'],
                'full_title': movie.get('full_title', movie['title']),
                'year': movie.get('year'),
                'avgRating': movie.get('avgRating') or movie.get('overallAvgRating'),
                'source': item['source'],
                'score': item['score']
            })
        
        return results
    
    def get_popular_recommendations(self, user_id: int, limit: int = 5) -> List[Dict]:
        """
        Fallback: Recommend popular movies for users with few/no ratings.
        """
        with self.driver.session() as session:
            result = session.run("""
                MATCH (m:Movie)
                WHERE m.ratingCount >= 50
                  AND NOT EXISTS {
                      MATCH (u:User {userId: $userId})-[:RATED]->(m)
                  }
                RETURN m.movieId as movieId,
                       m.title as title,
                       m.full_title as full_title,
                       m.year as year,
                       m.avgRating as avgRating,
                       m.ratingCount as ratingCount
                ORDER BY m.avgRating DESC, m.ratingCount DESC
                LIMIT $limit
            """, userId=user_id, limit=limit)
            
            return [dict(record) for record in result]
    
    def get_recommendations(self, user_id: int, limit: int = 5) -> List[Dict]:
        """
        Main recommendation method. Automatically selects the best strategy
        based on available data.
        """
        # Check how many ratings the user has
        with self.driver.session() as session:
            result = session.run("""
                MATCH (u:User {userId: $userId})-[:RATED]->()
                RETURN count(*) as ratingCount
            """, userId=user_id)
            
            rating_count = result.single()['ratingCount']
        
        # Choose strategy based on available data
        if rating_count == 0:
            # New user - use popular movies
            return self.get_popular_recommendations(user_id, limit)
        elif rating_count < 5:
            # Few ratings - prefer content-based
            return self.get_content_based_recommendations(user_id, limit)
        else:
            # Enough data - use hybrid approach
            return self.get_hybrid_recommendations(user_id, limit)


# Example usage
if __name__ == "__main__":
    neo4jUrl = "bolt://localhost:7687"
    recommender = MovieRecommender(
        uri=neo4jUrl,
        auth=('neo4j', 'password')
    )
    
    try:
        # Test with user ID 1
        user_id = 1
        
        print(f"Getting recommendations for user {user_id}...")
        recommendations = recommender.get_recommendations(user_id, limit=5)
        
        print("\nTop 5 Recommended Movies:")
        for i, rec in enumerate(recommendations, 1):
            print(f"{i}. {rec['title']} ({rec.get('year', 'N/A')})")
            print(f"   Avg Rating: {rec.get('avgRating', 0):.2f}")
            print(f"   Source: {rec.get('source', 'N/A')}")
            print()
        
    finally:
        recommender.close()