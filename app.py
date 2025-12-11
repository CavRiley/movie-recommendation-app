from flask import Flask, render_template, request, redirect, url_for, session, flash
from pathlib import Path
from redis import Redis
from neo4j import GraphDatabase
from db.redis_queries import search_movies, cache_user_ratings, get_cached_user_ratings, get_random_movie
from db.neo4j_recommender import MovieRecommender
import os

ROOT = Path(__file__).resolve().parent
TEMPLATES_DIR = ROOT / "templates"
STATIC_DIR = ROOT / "static"

app = Flask(
    __name__,
    template_folder=str(TEMPLATES_DIR),
    static_folder=str(STATIC_DIR),
)

# Secret key for sessions
app.secret_key = os.urandom(24)

# Database connections
r = Redis(host="localhost", port=6379, db=0, decode_responses=True)
neo4jUrl = "bolt://localhost:7687"
neo4jDriver = GraphDatabase.driver(neo4jUrl, auth=('neo4j', 'password'))
recommender = MovieRecommender(uri=neo4jUrl, auth=('neo4j', 'password'))


@app.route("/")
def home():
    """Landing page - redirect to login if not logged in"""
    if 'user_id' not in session:
        return redirect(url_for('login'))
    return redirect(url_for('dashboard'))


@app.route("/login", methods=['GET', 'POST'])
def login():
    """User login/registration page"""
    if request.method == 'POST':
        user_id = request.form.get('user_id')
        
        if not user_id or not user_id.isdigit():
            flash('Please enter a valid user ID', 'error')
            return render_template('login.html')
        
        user_id = int(user_id)
        
        # Check if user exists in Neo4j
        user_info = recommender.get_or_create_user(user_id)
        
        if user_info['exists'] and user_info.get('name'):
            # Existing user with name
            session['user_id'] = user_id
            session['user_name'] = user_info['name']
            flash(f'Welcome back, {user_info["name"]}!', 'success')
            return redirect(url_for('dashboard'))
        elif user_info['exists']:
            # Existing user without name - ask for it
            session['user_id'] = user_id
            return redirect(url_for('set_name'))
        else:
            # New user - ask for name
            session['user_id'] = user_id
            return redirect(url_for('set_name'))
    
    return render_template('login.html')


@app.route("/set_name", methods=['GET', 'POST'])
def set_name():
    """Set user name for new users"""
    if 'user_id' not in session:
        return redirect(url_for('login'))
    
    if request.method == 'POST':
        name = request.form.get('name')
        
        if not name or not name.strip():
            flash('Please enter a valid name', 'error')
            return render_template('set_name.html')
        
        user_id = session['user_id']
        
        # Create/update user with name in Neo4j
        user_info = recommender.get_or_create_user(user_id, name.strip())
        session['user_name'] = name.strip()
        
        flash(f'Welcome, {name}!', 'success')
        return redirect(url_for('dashboard'))
    
    return render_template('set_name.html')


@app.route("/dashboard")
def dashboard():
    """Main dashboard showing user's rated movies and recommendations"""
    if 'user_id' not in session:
        return redirect(url_for('login'))
    
    user_id = session['user_id']
    user_name = session.get('user_name', f'User {user_id}')
    
    # Try to get cached ratings from Redis first
    cached_ratings = get_cached_user_ratings(r, user_id)
    
    if cached_ratings is None:
        # Not in cache - get from Neo4j and cache
        user_ratings = recommender.get_user_ratings(user_id)
        cache_user_ratings(r, user_id, user_ratings, expire_seconds=1800)  # 30 min TTL
    else:
        user_ratings = cached_ratings
    
    # Get recommendations
    recommendations = recommender.get_recommendations(user_id, limit=5)
    
    # Enrich recommendations with genre info from Redis
    for rec in recommendations:
        movie_key = f"movie:{rec['movieId']}"
        movie_data = r.hgetall(movie_key)
        if movie_data:
            rec['genres'] = movie_data.get('genre', '').replace(' ', ', ')
    
    # Enrich user ratings with genre info from Redis
    for rating in user_ratings:
        movie_key = f"movie:{rating['movieId']}"
        movie_data = r.hgetall(movie_key)
        if movie_data:
            rating['genres'] = movie_data.get('genre', '').replace(' ', ', ')
    
    return render_template(
        'dashboard.html',
        user_name=user_name,
        user_ratings=user_ratings,
        recommendations=recommendations
    )


@app.route("/search")
def search():
    """Search for movies using Redis full-text search"""
    if 'user_id' not in session:
        return redirect(url_for('login'))
    
    search_term = request.args.get("term", "").strip()
    limit = int(request.args.get("limit", 10))
    sort_by = request.args.get("sort_by", "relevance")  # relevance, rating_high, rating_low, title_asc, title_desc, year_asc, year_desc
    
    # Validate limit
    valid_limits = [10, 25, 50, 100]
    if limit not in valid_limits:
        limit = 10
    
    if not search_term:
        flash('Please enter a search term', 'error')
        return redirect(url_for('dashboard'))
    
    user_id = session['user_id']
    
    # Get user's rated movies to check if they've seen each result
    user_ratings_dict = {}
    cached_ratings = get_cached_user_ratings(r, user_id)
    if cached_ratings:
        user_ratings_dict = {rating['movieId']: rating['rating'] for rating in cached_ratings}
    else:
        user_ratings = recommender.get_user_ratings(user_id)
        user_ratings_dict = {rating['movieId']: rating['rating'] for rating in user_ratings}
    
    # Search movies in Redis
    search_results = search_movies(redis=r, term=search_term)
    
    if not search_results or not hasattr(search_results, 'docs'):
        flash('No movies found', 'info')
        return redirect(url_for('dashboard'))
    
    movies = []
    movie_ids = []
    # Get all results first (we'll limit after sorting)
    for doc in search_results.docs:
        movie_id_str = doc.id.split(':')[-1]
        try:
            movie_id = int(movie_id_str)
        except ValueError:
            continue
        
        movie_ids.append(movie_id)
        user_rating = user_ratings_dict.get(movie_id)
        has_seen = movie_id in user_ratings_dict
        
        movies.append({
            "id": movie_id,
            "title": doc.title,
            "genres": doc.genre.replace(' ', ', '),
            "avg_rating": float(doc.avg_rating),
            "year": None,  # Will be populated if needed
            "has_seen": has_seen,
            "user_rating": user_rating
        })
    
    # Fetch years from Neo4j in batch if needed for sorting
    if sort_by in ['year_asc', 'year_desc'] and movie_ids:
        year_map = {}
        with neo4jDriver.session() as session_db:
            result = session_db.run("""
                MATCH (m:Movie)
                WHERE m.movieId IN $movieIds
                RETURN m.movieId as movieId, m.year as year
            """, movieIds=movie_ids)
            for record in result:
                year_map[record['movieId']] = record['year']
        
        # Update movies with year data
        for movie in movies:
            movie['year'] = year_map.get(movie['id'])
    
    # Apply sorting
    if sort_by == "rating_high":
        movies.sort(key=lambda x: x['avg_rating'], reverse=True)
    elif sort_by == "rating_low":
        movies.sort(key=lambda x: x['avg_rating'])
    elif sort_by == "title_asc":
        movies.sort(key=lambda x: x['title'].lower())
    elif sort_by == "title_desc":
        movies.sort(key=lambda x: x['title'].lower(), reverse=True)
    elif sort_by == "year_asc":
        movies.sort(key=lambda x: x['year'] if x['year'] else 0)
    elif sort_by == "year_desc":
        movies.sort(key=lambda x: x['year'] if x['year'] else 9999, reverse=True)
    # "relevance" keeps the original order from Redis search
    
    # Apply limit after sorting
    movies = movies[:limit]
    
    return render_template('search_results.html', 
                         movies=movies, 
                         search_term=search_term,
                         limit=limit,
                         sort_by=sort_by)


@app.route("/random")
def random_movie():
    """Get a random movie and show it in search results"""
    if 'user_id' not in session:
        return redirect(url_for('login'))
    
    user_id = session['user_id']
    
    # Get random movie from Redis
    random_movie_data = get_random_movie(r)
    
    if not random_movie_data:
        flash('Could not find a random movie', 'error')
        return redirect(url_for('dashboard'))
    
    # Get user's rated movies
    user_ratings_dict = {}
    cached_ratings = get_cached_user_ratings(r, user_id)
    if cached_ratings:
        user_ratings_dict = {rating['movieId']: rating['rating'] for rating in cached_ratings}
    else:
        user_ratings = recommender.get_user_ratings(user_id)
        user_ratings_dict = {rating['movieId']: rating['rating'] for rating in user_ratings}
    
    # Get year from Neo4j
    year = None
    with neo4jDriver.session() as session_db:
        result = session_db.run("""
            MATCH (m:Movie {movieId: $movieId})
            RETURN m.year as year
        """, movieId=random_movie_data['movieId'])
        record = result.single()
        if record:
            year = record['year']
    
    movie = {
        "id": random_movie_data['movieId'],
        "title": random_movie_data['title'],
        "genres": random_movie_data['genres'].replace(' ', ', '),
        "avg_rating": random_movie_data['avg_rating'],
        "year": year,
        "has_seen": random_movie_data['movieId'] in user_ratings_dict,
        "user_rating": user_ratings_dict.get(random_movie_data['movieId'])
    }
    
    return render_template('search_results.html',
                         movies=[movie],
                         search_term="Random Movie",
                         limit=10,
                         sort_by="relevance")


@app.route("/rate/<int:movie_id>", methods=['POST'])
def rate_movie(movie_id):
    """Add/update a rating for a movie"""
    if 'user_id' not in session:
        return redirect(url_for('login'))
    
    user_id = session['user_id']
    rating = request.form.get('rating')
    
    try:
        rating = float(rating)
        if rating < 0.5 or rating > 5.0:
            flash('Rating must be between 0.5 and 5.0', 'error')
            return redirect(request.referrer or url_for('dashboard'))
    except (TypeError, ValueError):
        flash('Invalid rating value', 'error')
        return redirect(request.referrer or url_for('dashboard'))
    
    # Add rating to Neo4j
    success = recommender.add_rating(user_id, movie_id, rating)
    
    if success:
        # Update average rating in Redis
        movie_key = f"movie:{movie_id}"
        with neo4jDriver.session() as session_db:
            result = session_db.run("""
                MATCH (m:Movie {movieId: $movieId})
                RETURN m.avgRating as avgRating
            """, movieId=movie_id)
            record = result.single()
            if record:
                r.hset(movie_key, 'avg_rating', record['avgRating'])
        
        # Invalidate user ratings cache
        cache_key = f"user_ratings:{user_id}"
        r.delete(cache_key)
        
        flash('Rating submitted successfully!', 'success')
    else:
        flash('Error submitting rating', 'error')
    
    return redirect(request.referrer or url_for('dashboard'))


@app.route("/logout")
def logout():
    """Log out the user"""
    session.clear()
    flash('You have been logged out', 'info')
    return redirect(url_for('login'))


# Note: Database connections (Redis, Neo4j) are kept open for the app lifetime.
# They will be automatically closed when the application shuts down.
# Closing them after each request (via teardown_appcontext) would cause
# "Driver closed" errors on subsequent requests.


if __name__ == "__main__":
    app.run(debug=True)