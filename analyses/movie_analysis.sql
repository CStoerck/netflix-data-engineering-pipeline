WITH ratings_summary AS (
  SELECT
    movie_id,
    AVG(rating) AS average_rating,
    COUNT(*) AS total_ratings
  FROM {{ ref('fct_ratings') }}
  GROUP BY movie_id
  HAVING COUNT(*) > 100 -- Only movies with at least 100 ratings
)
SELECT
  m.movie_title,
  rs.average_rating,
  rs.total_ratings
FROM ratings_summary rs
JOIN {{ ref('dim_movies') }} m ON m.movie_id = rs.movie_id
ORDER BY rs.average_rating DESC
LIMIT 20;

-- Rating Distribution Across Genres
SELECT
  genre
  ,AVG(rating) AS average_rating
  ,COUNT(DISTINCT movie_id) AS total_movies
FROM {{ref('dim_movies_with_tags')}} m
JOIN {{ref('fct_ratings')}} r ON m.movie_id = r.movie_id
CROSS JOIN UNNEST(m.genre_array) AS genre
GROUP BY genre
ORDER BY average_rating DESC;

-- User Engagement (Number of Ratings per User)
SELECT
  user_id
  ,COUNT(*) AS numer_of_ratings
  ,AVG(rating) AS average_rating_given
FROM {{ref('fct_ratings')}}
GROUP BY user_id
ORDER BY numer_of_ratings DESC
LIMIT 20;

-- Trends of Movie Releases Over Time
SELECT
  EXTRACT(YEAR FROM release_date) AS release_year
  ,COUNT(DISTINCT movie_id) AS movies_released
FROM {{ref('seed_movie_release_dates')}}
GROUP BY release_year
ORDER BY release_year DESC;

-- Tag Relevance Analysis
SELECT
  t.tag_name
  ,AVG(gs.relevance_score) AS average_relevance
  ,COUNT(DISTINCT gs.movie_id) AS total_movies_tagged
FROM {{ref('fct_genome_scores')}} gs
JOIN {{ref('dim_genome_tags')}} t ON gs.tag_id = t.tag_id
GROUP BY t.tag_name
ORDER BY average_relevance DESC
LIMIT 20;

