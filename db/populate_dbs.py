from pathlib import Path
from neo4j import GraphDatabase
import pandas as pd
import re


# get neo4j driver
def get_driver(uri="bolt://localhost:7687", auth=('neo4j', 'password')):
    return GraphDatabase.driver(uri, auth=auth)

# close it
def close_driver(driver):
    driver.close()

# clear database before populating
def clear_database(driver):
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
        print("Database cleared.")

# for better performance
def create_constraints_and_indexes(driver):
    with driver.session() as session:
        # constraints ensure uniqueness and create indexes
        session.run("CREATE CONSTRAINT movie_id IF NOT EXISTS FOR (m:Movie) REQUIRE m.movieId IS UNIQUE")
        session.run("CREATE CONSTRAINT user_id IF NOT EXISTS FOR (u:User) REQUIRE u.userId IS UNIQUE")
        session.run("CREATE CONSTRAINT genre_name IF NOT EXISTS FOR (g:Genre) REQUIRE g.name IS UNIQUE")
        
        # additional indexes for performance
        session.run("CREATE INDEX movie_title IF NOT EXISTS FOR (m:Movie) ON (m.title)")
        session.run("CREATE INDEX movie_year IF NOT EXISTS FOR (m:Movie) ON (m.year)")
        
        print("Constraints and indexes created.")

# read/parse movies csv
def parse_movies_csv(movies_path="movies.csv"):
    movies_data = []
    genres_set = set()
    
    # read raw lines to handle malformed CSV
    with open(movies_path, "r", encoding="latin-1") as f:
        lines = f.readlines()
    
    # regex for year
    year_pattern = re.compile(r'\(\d{4}\)')
    i = 0
    
    while i < len(lines):
        line = lines[i].strip()
        if not line or line.startswith('movieId'):  # skip empty lines and header
            i += 1
            continue
        
        # split by commas
        parts = line.split(',')
        
        # check if we have at least movieId
        if len(parts) < 2:
            i += 1
            continue
        
        try:
            movie_id = int(parts[0])
        except ValueError:
            i += 1
            continue
        
        # find the part with year (YYYY) in parentheses - this marks end of title
        title_parts = []
        genres_str = ""
        
        found_year = False
        for j in range(1, len(parts)):
            part = parts[j].strip()
            if not found_year:
                title_parts.append(part)
                # check if this part contains the year
                if year_pattern.search(part):
                    found_year = True
            else:
                # everything after year is genres
                genres_str = part
                break
        
        # join title parts with comma (since we split on comma)
        if not title_parts:
            i += 1
            continue
            
        # reconstruct title
        full_title = ', '.join(title_parts).strip().strip('"')
        genres = genres_str.strip().split("|") if genres_str else []
        
        # extract year from title if present (format: "Title (YYYY)")
        year = None
        title_without_year = full_title
        year_match = year_pattern.search(full_title)
        if year_match:
            try:
                year = int(year_match.group(0)[1:5])  # extract just the 4 digits
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
    
    return movies_data, genres_set

# create genre nodes
def create_genre_nodes(driver, genres_set):
    with driver.session() as session:
        for genre in genres_set:
            if genre and genre != "(no genres listed)":
                session.run(
                    "MERGE (g:Genre {name: $name})",
                    name=genre
                )
        print(f"Created {len(genres_set)} genre nodes.")

# create movie nodes
def create_movie_nodes(driver, movies_data, batch_size=1000):
    with driver.session() as session:
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

# create movie-genre relationships
def create_movie_genre_relationships(driver, movies_data):
    with driver.session() as session:
        for movie in movies_data:
            for genre in movie['genres']:
                if genre and genre != "(no genres listed)":
                    session.run("""
                        MATCH (m:Movie {movieId: $movieId})
                        MATCH (g:Genre {name: $genre})
                        MERGE (m)-[:HAS_GENRE]->(g)
                    """, movieId=movie['movieId'], genre=genre)
        print("Created Movie-Genre relationships.")

# import movies and genres
def import_movies(driver, movies_path="movies.csv"):
    movies_data, genres_set = parse_movies_csv(movies_path)
    create_genre_nodes(driver, genres_set)
    create_movie_nodes(driver, movies_data)
    create_movie_genre_relationships(driver, movies_data)

# create user nodes
def create_user_nodes(driver, unique_users):
    with driver.session() as session:
        for user_id in unique_users:
            session.run(
                "MERGE (u:User {userId: $userId})",
                userId=int(user_id)
            )
        print(f"Created {len(unique_users)} user nodes.")

# create rated relationships
def create_rated_relationships(driver, ratings_list, batch_size=5000):
    with driver.session() as session:
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

# import ratings and create user nodes
def import_ratings(driver, ratings_path="ratings.csv"):
    # read ratings
    ratings_df = pd.read_csv(ratings_path)
    
    # get unique users
    unique_users = ratings_df['userId'].unique()
    
    # create user nodes
    create_user_nodes(driver, unique_users)
    
    # create rated relationships
    ratings_list = ratings_df.to_dict('records')
    create_rated_relationships(driver, ratings_list)


# compute movie statistics
def compute_movie_statistics(driver):
    with driver.session() as session:
        # add average rating and rating count to each movie
        session.run("""
            MATCH (m:Movie)<-[r:RATED]-()
            WITH m, avg(r.rating) as avgRating, count(r) as ratingCount
            SET m.avgRating = avgRating,
                m.ratingCount = ratingCount
        """)
        
        # set 0 for movies with no ratings
        session.run("""
            MATCH (m:Movie)
            WHERE m.avgRating IS NULL
            SET m.avgRating = 0.0, m.ratingCount = 0
        """)
        
        print("Computed movie statistics (avgRating, ratingCount).")

# fetch movies from neo4j
def fetch_movies_from_neo4j(driver):
    movies = []
    with driver.session() as session:
        results = session.run("""
            MATCH (m:Movie)
            OPTIONAL MATCH (m)-[:HAS_GENRE]->(g:Genre)
            WITH m, collect(g.name) AS genres
            RETURN m.movieId AS movieId,
                   m.title AS title,
                   genres,
                   coalesce(m.avgRating, 0.0) AS avgRating
        """)
        
        for record in results:
            movies.append({
                'movieId': record["movieId"],
                'title': record["title"] or "",
                'genres': record["genres"] or [],
                'avgRating': float(record["avgRating"])
            })
    
    return movies


# load movies to redis
def load_movies_to_redis(redis_client, movies):
    count = 0
    for movie in movies:
        redis_client.hset(
            name=f"movie:{movie['movieId']}",
            mapping={
                "title": movie['title'],
                "genre": " ".join(movie['genres']),
                "avg_rating": movie['avgRating'],
            }
        )
        count += 1
    
    print(f"✔ Loaded {count} movies into Redis.")


# create redis search index
def create_redis_search_index(redis_client):
    try:
        redis_client.execute_command(
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
        if "Index already exists" in str(e):
            print("Index movies_index already exists — skipping.")
        else:
            raise

# populate redis
def populate_redis(driver, redis_host="localhost", redis_port=6379, redis_db=0):
    from redis import Redis
    
    redis_client = Redis(host=redis_host, port=redis_port, db=redis_db)
    
    print("Fetching movies + genres + avg ratings from Neo4j...")
    movies = fetch_movies_from_neo4j(driver)
    
    load_movies_to_redis(redis_client, movies)
    create_redis_search_index(redis_client)
    
    redis_client.quit()
    print("Redis connection closed.")

# run all functions
def main():
    # connection configuration
    neo4j_url = "bolt://localhost:7687"
    neo4j_auth = ('neo4j', 'password')
    
    # create driver
    driver = get_driver(uri=neo4j_url, auth=neo4j_auth)
    
    try:
        print("Starting MovieLens data import to Neo4j...")
        
        # optional: clear existing data
        clear_database(driver)
        
        # step 1: create constraints and indexes
        create_constraints_and_indexes(driver)
        
        # step 2: import movies and genres
        print("\nImporting movies and genres...")
        import_movies(driver, "movies.csv")
        
        # step 3: import ratings and users
        print("\nImporting ratings and users...")
        import_ratings(driver, "ratings.csv")
        
        # step 4: compute statistics
        print("\nComputing movie statistics...")
        compute_movie_statistics(driver)
        
        # step 5: load into redis
        print("\nLoading movies into Redis...")
        populate_redis(driver)
        
        print("\n✓ Data import completed successfully!")
        
    finally:
        close_driver(driver)

# run code
if __name__ == "__main__":
    main()