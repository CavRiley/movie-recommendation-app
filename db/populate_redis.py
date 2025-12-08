import csv
import re
from pathlib import Path
from redis import Redis
import pandas as pd


MOVIES_PATH = Path("../movies.csv")
data_csv = open(MOVIES_PATH, "r", encoding="latin-1")

r = Redis(host="localhost", port=6379, db=0)

RATINGS_PATH = Path("../ratings.csv")
ratings = pd.read_csv(RATINGS_PATH)
avg_ratings = (
    ratings.groupby("movieId")["rating"]
    .mean()
    .to_dict()
)

def parse_movie_line(line: str):
    """
    Try to parse a line like:
      1356,Star Trek, First Contact (1996),Action|Adventure|Sci-Fi,
    into:
      movie_id = 1356
      title = Star Trek, First Contact (1996)
      genres = Action|Adventure|Sci-Fi
    Returns (movie_id, title, genres) or None if unparseable.
    """
    line = line.strip("\n\r")
    if not line:
        return None

    # First split movie_id (first comma)
    parts = line.split(",", 1)
    if len(parts) != 2:
        return None

    movie_id_raw, rest = parts
    movie_id = movie_id_raw.strip()
    rest = rest.strip()

    # Now try to find title: assume title ends at the *first closing parenthesis + the next comma*
    # e.g. “Star Trek, First Contact (1996),Action|…”
    # regex: match up to ")"
    m = re.match(r"(?P<title>.*\)\s*)(?P<after>.*)", rest)
    if m:
        title = m.group("title").rstrip(", ")  # remove trailing comma/spaces
        if title[0] == '"':
            title = title[1:]
        after = m.group("after").lstrip(", ")
        # after should be the genres (and maybe other columns)
        list_afters = after.split(",")
        genre = list_afters[0] if list_afters[0] != '"' else list_afters[1]
        genres = genre
    else:
        # fallback: naive split on comma
        fields = rest.split(",")
        if len(fields) < 2:
            # unparseable
            return None
        title = fields[0].strip()
        genres = fields[1].strip()

    return movie_id, title, genres

with open(MOVIES_PATH, "r", encoding="latin-1") as f:
    header = f.readline()  # skip header

    for idx, line in enumerate(f, start=2):
        parsed = parse_movie_line(line)
        print(parsed)
        if not parsed:
            print(f"[WARN] Could not parse line {idx}: {line!r}")
            continue

        movie_id, title, genres = parsed
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
