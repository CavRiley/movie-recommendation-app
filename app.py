from db.redis_queries import search_movies
from flask import Flask, render_template, request
from pathlib import Path
import os
from redis import Redis

ROOT = Path(__file__).resolve().parent
TEMPLATES_DIR = ROOT / "templates"
STATIC_DIR = ROOT / "static"

app = Flask(
    __name__,
    template_folder=str(TEMPLATES_DIR),
    static_folder=str(STATIC_DIR),
)

r = Redis(host="localhost", port=6379, db=0, decode_responses=True)


@app.route("/")
def home():
    # Grab a few movie hashes from Redis: movie:<movieId>
    keys = r.keys("movie:*")
    movies = []

    # Just take the first 8 keys for the landing page
    for key in keys[:8]:
        data = r.hgetall(key)
        if not data:
            continue

        movie_id = key.split("movie:")[-1]
        title = data.get("title", f"Movie {movie_id}")
        genres = data.get("genres") or data.get("genre") or ""
        avg_rating_raw = data.get("avg_rating") or data.get("average_rating")

        try:
            avg_rating = float(avg_rating_raw) if avg_rating_raw is not None else None
        except (TypeError, ValueError):
            avg_rating = None

        movies.append(
            {
                "id": movie_id,
                "title": title,
                "genres": genres.replace("|", ", "),
                "avg_rating": avg_rating,
            }
        )

    # Sort so highest-rated movies float to the top, unrated at the end
    movies.sort(key=lambda m: (m["avg_rating"] is None, -(m["avg_rating"] or 0)))

    return render_template("home.html", movies=movies)

@app.route("/search")
def search():
    search_term = request.args.get("term", "")

    if not search_term:
        return

    documents = search_movies(redis=r, term=search_term).docs

    movies = []
    num_movies = 0
    for document in documents:
        # display 10 max
        if num_movies > 9:
            break

        movies.append(
            {
                "id": document.id,
                "title": document.title,
                "genres": document.genre.replace("|", ", "),
                "avg_rating": float(document.avg_rating),
            }
        )

        num_movies += 1


    return render_template("home.html", movies=movies)



if __name__ == "__main__":
    app.run(debug=True)
