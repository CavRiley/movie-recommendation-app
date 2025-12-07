from flask import Flask, render_template
from pathlib import Path
import os
import redis

ROOT = Path(__file__).resolve().parent
TEMPLATES_DIR = ROOT / "templates"
STATIC_DIR = ROOT / "static"

app = Flask(
    __name__,
    template_folder=str(TEMPLATES_DIR),
    static_folder=str(STATIC_DIR),
)


def get_redis_client() -> redis.Redis:
    """
    Return a Redis client.

    Uses environment variables if set, otherwise defaults to localhost:6379, db 0.
    """
    return redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", 6379)),
        db=int(os.getenv("REDIS_DB", 0)),
        decode_responses=True,  # return strings instead of bytes
    )


@app.route("/")
def home():
    r = get_redis_client()

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


if __name__ == "__main__":
    app.run(debug=True)
