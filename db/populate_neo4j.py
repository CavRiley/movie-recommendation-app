from pathlib import Path
from neo4j import GraphDatabase
import pandas as pd


class Neo4jMovieLensImporter:
    def __init__(self, uri="bolt://localhost:7687", auth=('neo4j', 'password')):
        self.driver = GraphDatabase.driver(uri, auth=auth)
    
    def close(self):
        self.driver.close()
    
    def clear_database(self):
        """Clear existing data (optional - use with caution!)"""
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            print("Database cleared.")
    
    def create_constraints_and_indexes(self):
        """Create constraints and indexes for optimal query performance"""
        with self.driver.session() as session:
            # Constraints ensure uniqueness and create indexes
            session.run("CREATE CONSTRAINT movie_id IF NOT EXISTS FOR (m:Movie) REQUIRE m.movieId IS UNIQUE")
            session.run("CREATE CONSTRAINT user_id IF NOT EXISTS FOR (u:User) REQUIRE u.userId IS UNIQUE")
            session.run("CREATE CONSTRAINT genre_name IF NOT EXISTS FOR (g:Genre) REQUIRE g.name IS UNIQUE")
            
            # Additional indexes for performance
            session.run("CREATE INDEX movie_title IF NOT EXISTS FOR (m:Movie) ON (m.title)")
            session.run("CREATE INDEX movie_year IF NOT EXISTS FOR (m:Movie) ON (m.year)")
            
            print("Constraints and indexes created.")
    
    def import_movies(self, movies_path="movies.csv"):
        """Import movies and genres"""
        movies_data = []
        genres_set = set()
        
        # Read and parse movies
        with open(movies_path, "r", encoding="latin-1") as f:
            # Read raw lines first to handle malformed CSV
            lines = f.readlines()
        
        # Parse lines manually to handle split titles
        import re
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            if not line or line.startswith('movieId'):  # Skip empty lines and header
                i += 1
                continue
            
            # Split by comma
            parts = line.split(',')
            
            # Check if we have at least movieId
            if len(parts) < 2:
                i += 1
                continue
            
            try:
                movie_id = int(parts[0])
            except ValueError:
                i += 1
                continue
            
            # Find the part with year (YYYY) in parentheses - this marks end of title
            title_parts = []
            genres_str = ""
            year_pattern = re.compile(r'\(\d{4}\)')
            
            found_year = False
            for j in range(1, len(parts)):
                part = parts[j].strip()
                if not found_year:
                    title_parts.append(part)
                    # Check if this part contains the year
                    if year_pattern.search(part):
                        found_year = True
                else:
                    # Everything after year is genres
                    genres_str = part
                    break
            
            # Join title parts with comma (since we split on comma)
            if not title_parts:
                i += 1
                continue
                
            # Reconstruct title - join with comma and space, but clean up extra spaces
            full_title = ', '.join(title_parts)
            # Remove leading/trailing quotes if present
            full_title = full_title.strip().strip('"')
            
            genres = genres_str.strip().split("|") if genres_str else []
            
            # Extract year from title if present (format: "Title (YYYY)")
            year = None
            title_without_year = full_title
            year_match = year_pattern.search(full_title)
            if year_match:
                try:
                    year = int(year_match.group(0)[1:5])  # Extract just the 4 digits
                    title_without_year = full_title[:year_match.start()].strip()
                except ValueError:
                    pass
            
            movies_data.append({
                'movieId': movie_id,
                'title': title_without_year,
                'full_title': full_title,
                'year': year,
                'genres': genres
            })
            
            genres_set.update(genres)
            i += 1
        
        # Create Genre nodes
        with self.driver.session() as session:
            for genre in genres_set:
                if genre and genre != "(no genres listed)":
                    session.run(
                        "MERGE (g:Genre {name: $name})",
                        name=genre
                    )
            print(f"Created {len(genres_set)} genre nodes.")
        
        # Create Movie nodes in batches
        batch_size = 1000
        with self.driver.session() as session:
            for i in range(0, len(movies_data), batch_size):
                batch = movies_data[i:i + batch_size]
                session.run("""
                    UNWIND $movies AS movie
                    CREATE (m:Movie {
                        movieId: movie.movieId,
                        title: movie.title,
                        full_title: movie.full_title,
                        year: movie.year
                    })
                """, movies=batch)
            print(f"Created {len(movies_data)} movie nodes.")
        
        # Create Movie-Genre relationships
        with self.driver.session() as session:
            for movie in movies_data:
                for genre in movie['genres']:
                    if genre and genre != "(no genres listed)":
                        session.run("""
                            MATCH (m:Movie {movieId: $movieId})
                            MATCH (g:Genre {name: $genre})
                            MERGE (m)-[:HAS_GENRE]->(g)
                        """, movieId=movie['movieId'], genre=genre)
            print("Created Movie-Genre relationships.")
    
    def import_ratings(self, ratings_path="ratings.csv"):
        """Import ratings and create User nodes with RATED relationships"""
        # Read ratings
        ratings_df = pd.read_csv(ratings_path)
        
        # Get unique users
        unique_users = ratings_df['userId'].unique()
        
        # Create User nodes
        with self.driver.session() as session:
            for user_id in unique_users:
                session.run(
                    "MERGE (u:User {userId: $userId})",
                    userId=int(user_id)
                )
            print(f"Created {len(unique_users)} user nodes.")
        
        # Create RATED relationships in batches
        batch_size = 5000
        ratings_list = ratings_df.to_dict('records')
        
        with self.driver.session() as session:
            for i in range(0, len(ratings_list), batch_size):
                batch = ratings_list[i:i + batch_size]
                session.run("""
                    UNWIND $ratings AS rating
                    MATCH (u:User {userId: rating.userId})
                    MATCH (m:Movie {movieId: rating.movieId})
                    CREATE (u)-[:RATED {
                        rating: rating.rating,
                        timestamp: rating.timestamp
                    }]->(m)
                """, ratings=[{
                    'userId': int(r['userId']),
                    'movieId': int(r['movieId']),
                    'rating': float(r['rating']),
                    'timestamp': int(r['timestamp'])
                } for r in batch])
                
                if (i + batch_size) % 10000 == 0:
                    print(f"Processed {i + batch_size} ratings...")
        
        print(f"Created {len(ratings_list)} rating relationships.")
    
    def compute_movie_statistics(self):
        """Add computed statistics to movie nodes"""
        with self.driver.session() as session:
            # Add average rating and rating count to each movie
            session.run("""
                MATCH (m:Movie)<-[r:RATED]-()
                WITH m, avg(r.rating) as avgRating, count(r) as ratingCount
                SET m.avgRating = avgRating,
                    m.ratingCount = ratingCount
            """)
            
            # Set 0 for movies with no ratings
            session.run("""
                MATCH (m:Movie)
                WHERE m.avgRating IS NULL
                SET m.avgRating = 0.0, m.ratingCount = 0
            """)
            
            print("Computed movie statistics (avgRating, ratingCount).")


    def populate_redis(self, redis_host="localhost", redis_port=6379, redis_db=0):
        """
        Extract all movies from Neo4j and load them into Redis as hashes.
        Also creates a RediSearch full-text index over movie titles.

        Required by Part 3 of the project spec.
        """

        from redis import Redis

        r = Redis(host=redis_host, port=redis_port, db=redis_db)

        print("Fetching movies + genres + avg ratings from Neo4j...")

        with self.driver.session() as session:
            results = session.run("""
                MATCH (m:Movie)
                OPTIONAL MATCH (m)-[:HAS_GENRE]->(g:Genre)
                WITH m, collect(g.name) AS genres
                RETURN m.movieId AS movieId,
                       m.title AS title,
                       genres,
                       coalesce(m.avgRating, 0.0) AS avgRating
            """)

            count = 0
            for record in results:
                movie_id = record["movieId"]
                title = record["title"] or ""
                genres = record["genres"] or []
                avg_rating = float(record["avgRating"])

                # Store as a Redis hash (one movie = one hash)
                r.hset(
                    name=f"movie:{movie_id}",
                    mapping={
                        "title": title,
                        "genre": "|".join(genres),
                        "avg_rating": avg_rating,
                    }
                )
                count += 1

        print(f"✔ Loaded {count} movies into Redis.")

        # Create the RediSearch index (idempotent)
        try:
            r.execute_command(
                "FT.CREATE", "movies_index",
                "ON", "HASH",
                "PREFIX", "1", "movie:",
                "SCHEMA",
                "title", "TEXT",
                "genre", "TEXT",
                "avg_rating", "NUMERIC"
            )
            print("✔ Created RediSearch index movies_index.")
        except Exception as e:
            # Redis throws an error if index already exists — suppress it
            if "Index already exists" in str(e):
                print("Index movies_index already exists — skipping.")
            else:
                raise

        r.quit()
        print("Redis connection closed.")


def main():
    # Using the same connection pattern as your main app
    neo4jUrl = "bolt://localhost:7687"
    importer = Neo4jMovieLensImporter(
        uri=neo4jUrl,
        auth=('neo4j', 'password')
    )
    
    try:
        print("Starting MovieLens data import to Neo4j...")
        
        # Optional: Clear existing data
        importer.clear_database()
        
        # Step 1: Create constraints and indexes
        importer.create_constraints_and_indexes()
        
        # Step 2: Import movies and genres
        print("\nImporting movies and genres...")
        importer.import_movies("movies.csv")
        
        # Step 3: Import ratings and users
        print("\nImporting ratings and users...")
        importer.import_ratings("ratings.csv")
        
        # Step 4: Compute statistics
        print("\nComputing movie statistics...")
        importer.compute_movie_statistics()

        print("\nLoading movies into Redis...")
        importer.populate_redis()

        print("\n✓ Data import completed successfully!")
        
    finally:
        importer.close()


if __name__ == "__main__":
    main()