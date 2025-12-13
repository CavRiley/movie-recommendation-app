from neo4j import GraphDatabase
from typing import List, Dict, Optional

# movie recommender class
class MovieRecommender:
    # hybrid recommendation system using neo4j graph database
    # combines collaborative filtering and content-based filtering
    def __init__(self, uri="bolt://localhost:7687", auth=('neo4j', 'password')):
        self.driver = GraphDatabase.driver(uri, auth=auth)
    
    # close driver
    def close(self):
        self.driver.close()
    
    # get or create user
    def get_or_create_user(self, user_id: int, name: Optional[str] = None) -> Dict:
        # get existing user or create new one with name
        with self.driver.session() as session:
            # check if user exists
            result = session.run("""
                MATCH (u:User {userId: $userId})
                RETURN u.userId as userId, u.name as name
            """, userId=user_id)
            
            record = result.single()
            
            if record:
                # user exists - update name if provided
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
                # create new user
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
    
    # get user ratings
    def get_user_ratings(self, user_id: int) -> List[Dict]:
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
    
    # add rating
    def add_rating(self, user_id: int, movie_id: int, rating: float) -> bool:
        with self.driver.session() as session:
            try:
                # check if rating exists and update or create
                session.run("""
                    MATCH (u:User {userId: $userId})
                    MATCH (m:Movie {movieId: $movieId})
                    MERGE (u)-[r:RATED]->(m)
                    SET r.rating = $rating,
                        r.timestamp = timestamp()
                """, userId=user_id, movieId=movie_id, rating=rating)
                
                # update movie average rating
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
        # collaborative filtering using GDS COSINE SIMILARITY
        # uses the Graph Data Science library's built-in cosine similarity function
        with self.driver.session() as session:
            result = session.run("""
                // Find users who have rated movies in common with target user
                MATCH (target:User {userId: $userId})-[r1:RATED]->(m:Movie)<-[r2:RATED]-(other:User)
                WHERE target <> other
                
                // Collect rating vectors for each user pair
                WITH target, other,
                     collect(r1.rating) as targetRatings,
                     collect(r2.rating) as otherRatings,
                     count(m) as commonMovies
                WHERE commonMovies >= 3
                
                // Calculate cosine similarity using GDS function
                WITH target, other,
                     gds.similarity.cosine(targetRatings, otherRatings) as cosineSimilarity,
                     commonMovies
                WHERE cosineSimilarity > 0
                ORDER BY cosineSimilarity DESC
                LIMIT 20
                
                // Get highly-rated movies from similar users that target hasn't seen
                MATCH (other)-[r:RATED]->(rec:Movie)
                WHERE r.rating >= 3.5
                  AND NOT EXISTS {
                      MATCH (target)-[:RATED]->(rec)
                  }
                
                // Aggregate and rank recommendations
                // Weight by cosine similarity and rating
                WITH rec, 
                     sum(cosineSimilarity * r.rating) as score,
                     avg(r.rating) as avgRatingBySimilarUsers,
                     count(DISTINCT other) as recommendedBy,
                     avg(cosineSimilarity) as avgSimilarity,
                     rec.avgRating as overallAvgRating
                
                RETURN rec.movieId as movieId,
                       rec.title as title,
                       rec.full_title as full_title,
                       rec.year as year,
                       overallAvgRating,
                       avgRatingBySimilarUsers,
                       recommendedBy,
                       avgSimilarity,
                       score
                ORDER BY score DESC, overallAvgRating DESC
                LIMIT $limit
            """, userId=user_id, limit=limit)
            
            return [dict(record) for record in result]
    
    def get_content_based_recommendations(self, user_id: int, limit: int = 5) -> List[Dict]:
        # content-based filtering: recommend movies similar to those the user liked
        # strategy:
        # 1. find genres the user likes based on their ratings
        # 2. recommend highly-rated movies in those genres
        # 3. filter out movies already rated
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
        # hybrid approach: combine collaborative and content-based recommendations
        # this provides a balanced set of recommendations that considers both
        # what similar users like and what aligns with the user's preferences
        # get more recommendations from each method
        collab_recs = self.get_collaborative_recommendations(user_id, limit=limit * 2)
        content_recs = self.get_content_based_recommendations(user_id, limit=limit * 2)
        
        # create a combined ranking
        movie_scores = {}
        
        # add collaborative filtering scores (weighted higher for users with more ratings)
        for i, rec in enumerate(collab_recs):
            movie_id = rec['movieId']
            # higher position = higher score
            collab_score = (len(collab_recs) - i) * 2.0
            movie_scores[movie_id] = {
                'movie': rec,
                'score': collab_score,
                'collab_score': collab_score,
                'content_score': 0,
                'source': 'collaborative'
            }
        
        # add content-based scores
        for i, rec in enumerate(content_recs):
            movie_id = rec['movieId']
            content_score = (len(content_recs) - i) * 1.5
            
            if movie_id in movie_scores:
                # movie appears in both - boost its score
                movie_scores[movie_id]['score'] += content_score
                movie_scores[movie_id]['content_score'] = content_score
                movie_scores[movie_id]['source'] = 'hybrid'
            else:
                movie_scores[movie_id] = {
                    'movie': rec,
                    'score': content_score,
                    'collab_score': 0,
                    'content_score': content_score,
                    'source': 'content'
                }
        
        # sort by combined score and return top N
        sorted_recs = sorted(
            movie_scores.values(),
            key=lambda x: x['score'],
            reverse=True
        )[:limit]

        print(f"\n{'='*80}")
        print(f"Hybrid Recommendation Scores for User {user_id}")
        print(f"{'='*80}")
        print(f"{'Movie':<40} {'Collab':<10} {'Content':<10} {'Total':<10} {'Source':<12}")
        print(f"{'-'*80}")
        for item in sorted_recs:
            movie_title = item['movie']['title'][:37] + '...' if len(item['movie']['title']) > 40 else item['movie']['title']
            print(f"{movie_title:<40} {item['collab_score']:<10.1f} {item['content_score']:<10.1f} {item['score']:<10.1f} {item['source']:<12}")
        print(f"{'='*80}\n")
        
        # format the results
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
        # recommend popular movies for users with few/no ratings
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
        # check how many ratings the user has
        with self.driver.session() as session:
            result = session.run("""
                MATCH (u:User {userId: $userId})-[:RATED]->()
                RETURN count(*) as ratingCount
            """, userId=user_id)
            
            rating_count = result.single()['ratingCount']
        
        # choose strategy based on available data
        if rating_count == 0:
            # new user - use popular movies
            return self.get_popular_recommendations(user_id, limit)
        elif rating_count < 5:
            # few ratings - prefer content-based
            return self.get_content_based_recommendations(user_id, limit)
        else:
            # enough data - use hybrid approach
            return self.get_hybrid_recommendations(user_id, limit)


# example usage
if __name__ == "__main__":
    neo4jUrl = "bolt://localhost:7687"
    recommender = MovieRecommender(
        uri=neo4jUrl,
        auth=('neo4j', 'password')
    )
    
    try:
        # test with user ID 1
        user_id = 2
        
        print(f"Getting recommendations for user {user_id}...")
        recommendations = recommender.get_recommendations(user_id, limit=5)
        
        print("\nTop 5 Recommended Movies:")
        for i, rec in enumerate(recommendations, 1):
            print(f"{i}. {rec['title']} ({rec.get('year', 'N/A')})")
            print(f"   Avg Rating: {rec.get('avgRating', 0):.2f}")
            print(f"   Source: {rec.get('source', 'N/A')}")
            print()
    # close recommender
    finally:
        recommender.close()