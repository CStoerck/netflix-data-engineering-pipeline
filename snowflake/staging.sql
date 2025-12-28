USE WAREHOUSE COMPUTE_WH;
USE DATABASE MOVIELENS;
USE SCHEMA RAW;

CREATE STAGE netflixstage
URL = 's3://netflix-dataset-cstoerck'
CREDENTIALS = (AWS_KEY_ID='${AWS_KEY_ID}' AWS_SECRET_KEY='${AWS_SECRET_KEY}');

CREATE OR REPLACE TABLE raw_movies (
    movieID INTEGER,
    title STRING,
    genres STRING
);
COPY INTO raw_movies 
FROM '@netflixstage/ml-20m/movies.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

CREATE OR REPLACE TABLE raw_genome_scores (
    movieID INTEGER,
    tagID INTEGER,
    relevance FLOAT
);
COPY INTO raw_genome_scores 
FROM '@netflixstage/ml-20m/genome-scores.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

CREATE OR REPLACE TABLE raw_genome_tags (
    tagID INTEGER,
    tag STRING
);
COPY INTO raw_genome_tags
FROM '@netflixstage/ml-20m/genome-tags.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

CREATE OR REPLACE TABLE raw_links (
    movieId INTEGER,
    imdbId INTEGER,
    tmdbId INTEGER
);
COPY INTO raw_links
FROM '@netflixstage/ml-20m/links.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

CREATE OR REPLACE TABLE raw_ratings (
    userId INTEGER,
    movieId INTEGER,
    rating FLOAT,
    timestamp TIMESTAMP
);
COPY INTO raw_ratings
FROM '@netflixstage/ml-20m/ratings.csv'
FILE_FORMAT = (
    TYPE = 'CSV'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TIMESTAMP_FORMAT = 'epoch_seconds'
);

CREATE OR REPLACE TABLE raw_tags (
    userId INTEGER,
    movieId INTEGER,
    tag STRING,
    time TIMESTAMP
);
COPY INTO raw_tags
FROM '@netflixstage/ml-20m/tags.csv'
FILE_FORMAT = (
    TYPE = 'CSV'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
)
ON_ERROR = 'CONTINUE';


CREATE OR REPLACE TABLE raw_movies_metadata (
    title STRING,
    release_date DATE,
    revenue INTEGER,
    budget INTEGER
);
COPY INTO raw_movies_metadata 
FROM @netflixstage/ml-20m/movies_metadata.csv
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
ON_ERROR = 'CONTINUE';
