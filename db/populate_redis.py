import csv
from pathlib import Path
from redis import Redis
import pandas as pd


MOVIES_PATH = Path("movies.csv")
data_csv = csv.DictReader(open(MOVIES_PATH, "r", encoding="latin-1"))

r = Redis(host="localhost", port=6379, db=0)

RATINGS_PATH = Path("ratings.csv")
ratings = pd.read_csv(RATINGS_PATH)
avg_ratings = (
    ratings.groupby("movieId")["rating"]
    .mean()
    .to_dict()
)


for row in data_csv:
    movie_id = row['movieId']
    title = row['title']
    genres = row['genres'].replace("|", " ") # TODO: might want to look into how we are storing the genres

    r.hset(
        name=f"movie:{movie_id}",
        mapping={
            "title": title,
            "genre": genres, # This will store the genres as text (e.g. Animation Children's)
            "avg_rating": avg_ratings.get(int(movie_id), 0.0),
        },
    )

r.execute_command(
    "FT.CREATE", "movies_index",
    "ON", "HASH",
    "PREFIX", "1", "movie:",
    "SCHEMA",
    "title", "TEXT"
)

r.quit()
