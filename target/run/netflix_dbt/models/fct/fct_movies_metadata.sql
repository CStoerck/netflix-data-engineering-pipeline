
  
    

create or replace transient table MOVIELENS.DEV.fct_movies_metadata
    
    
    
    as (
WITH movies_metadata AS (
    SELECT 
        CASE
            WHEN release_date IS NOT NULL
            THEN CONCAT(TRIM(title), ' (', TO_CHAR(release_date, 'YYYY'), ')')
            ELSE TRIM(title)
        END AS title,
        revenue AS revenue,
        budget AS budget
    FROM MOVIELENS.RAW.RAW_MOVIES_METADATA
)

SELECT
    title,
    revenue,
    budget
FROM movies_metadata
    )
;


  