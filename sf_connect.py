import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import os
import warnings
warnings.filterwarnings(
    "ignore",
    message="pandas only supports SQLAlchemy connectable",
    category=UserWarning
)


ACCOUNT   = os.getenv("SNOWFLAKE_ACCOUNT")
USER      = os.getenv("SNOWFLAKE_USER")
PASSWORD  = os.getenv("SNOWFLAKE_PASSWORD")
ROLE      = os.getenv("SNOWFLAKE_ROLE")
WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
DATABASE  = os.getenv("SNOWFLAKE_DATABASE")
SCHEMA    = os.getenv("SNOWFLAKE_SCHEMA")

conn = snowflake.connector.connect(
    account=ACCOUNT,
    user=USER,
    password=PASSWORD,
    role=ROLE,
    warehouse=WAREHOUSE,
    database=DATABASE,
    schema=SCHEMA,
    # For SSO, use: authenticator="externalbrowser" and remove password
)


sql = """
    SELECT *
    FROM MOVIELENS.DEV.DIM_MOVIES
"""

df_movies = pd.read_sql(sql, conn)

genre_hierarchy = [
    "Documentary",
    "Musical",
    "Horror",
    "Thriller",
    "Action",
    "Comedy",
    "Drama",
    "Romance",
    "Crime",
    "Mystery",
    "Film-Noir",
    "Western",
    "Science Fiction",
    "Fantasy",
    "Adventure",
    "War",
    "History",
    "Sport",
    "Music",
    "Animation",
    "Children"
]

genre_rank = {g: i for i, g in enumerate(genre_hierarchy)}


synonyms = {
    "Sci-Fi": "Science Fiction",
    "Science fiction": "Science Fiction",
    "Children's": "Children",
    "Film Noir": "Film-Noir",      # ensure hyphenated form
}


def normalize_genre(g: str) -> str:
    if not isinstance(g, str):
        return g
    g = g.strip()
    g = synonyms.get(g, g)
    if g.lower() == "film-noir":
        return "Film-Noir"
    if g.lower() == "science fiction":
        return "Science Fiction"
    return g.title()


def pick_primary_from_genres_string(genres_str: str) -> str | None:
    if not isinstance(genres_str, str) or not genres_str.strip():
        return None
    parts = [normalize_genre(p) for p in genres_str.split("|") if p.strip()]
    if not parts:
        return None

    if len(parts) == 1:
        return parts[0]

    known = [p for p in parts if p in genre_rank]
    if known:
        return min(known, key=lambda p: genre_rank[p])

    return parts[0]

def pick_primary(row):
    ga = row.get("GENRE_ARRAY")
    gs = row.get("GENRES")

    if isinstance(ga, list) and ga:
        parts = [normalize_genre(x) for x in ga if isinstance(x, str) and x.strip()]
        if len(parts) == 1:
            return parts[0]
        known = [p for p in parts if p in genre_rank]
        return min(known, key=lambda p: genre_rank[p]) if known else (parts[0] if parts else None)

    if isinstance(ga, str) and ga.strip().startswith("["):
        try:
            import json
            parsed = json.loads(ga)
            parts = [normalize_genre(x) for x in parsed if isinstance(x, str) and x.strip()]
            if len(parts) == 1:
                return parts[0]
            known = [p for p in parts if p in genre_rank]
            return min(known, key=lambda p: genre_rank[p]) if known else (parts[0] if parts else None)
        except Exception:
            pass
    return pick_primary_from_genres_string(gs)

df_movies["PRIMARY_GENRE"] = df_movies.apply(pick_primary, axis=1)


conn.cursor().execute("""
CREATE OR REPLACE TABLE MOVIELENS.DEV.DIM_MOVIES_ENRICHED (
    MOVIE_ID INTEGER,
    MOVIE_TITLE STRING,
    GENRES STRING,
    PRIMARY_GENRE STRING
)
""")

success, nchunks, nrows, _ = write_pandas(
    conn, df_movies[["MOVIE_ID","MOVIE_TITLE","GENRES","PRIMARY_GENRE"]],
    table_name="DIM_MOVIES_ENRICHED",)
