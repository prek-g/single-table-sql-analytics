DROP TABLE IF EXISTS netflix;
CREATE TABLE netflix 
(
    show_id VARCHAR(10) PRIMARY KEY,
    type VARCHAR(20),
    title VARCHAR(255),
    director VARCHAR(255),
    "cast" VARCHAR(1000),
    country VARCHAR(255),
    date_added VARCHAR(50),
    release_year INT,
    rating VARCHAR(10),
    duration VARCHAR(50),
    listed_in VARCHAR(255),
    description TEXT
);
-- COPY netflix FROM 'C:/Users/gjoni/Desktop/Netflix_SQL/netflix_titles.csv' DELIMITER ',' CSV HEADER;
SELECT * FROM netflix limit 100; 
SELECT COUNT(*) FROM netflix;

-- 1. Counting the number of Movies vs TV Shows

SELECT
    type,
    COUNT(*) AS total_types
FROM netflix
GROUP BY type

-- 2. Finding the Most Common Rating for Movies and TV Shows

SELECT 
    type,
    rating,
    COUNT(*) AS count_rating,
    RANK() OVER (PARTITION BY type ORDER BY COUNT(*) DESC) AS rating_rank
FROM netflix
GROUP BY type, rating
ORDER BY type, count_rating DESC;

-- Using CTE for better readability

WITH RankedRatings AS 
(
    SELECT 
        type,
        rating,
        COUNT(*) AS count_rating,
        RANK() OVER (PARTITION BY type ORDER BY COUNT(*) DESC) AS rating_rank
    FROM netflix
    GROUP BY type, rating
)
SELECT 
    type,
    rating,
    count_rating
FROM RankedRatings
WHERE rating_rank = 1;

-- 3. Listing all the movies released in 2020.

SELECT * FROM netflix
WHERE type = 'Movie' AND release_year = 2020;

/* 4. Finding the top 5 countries with the most content on netflix.  
One very important thing to tanke into consideration is that some rows have multiple countries seperated by a comma*/

SELECT
    TRIM(UNNEST(STRING_TO_ARRAY(country, ','))) as new_country, --always trim after splitting
    count(show_id) as total_content
FROM netflix
GROUP BY 1
ORDER BY 2 DESC;

/* 5. Identifying the longest movie
duration is a text, once again I can not use a max function on it right away.*/

SELECT
*
FROM netflix
WHERE type='Movie' 
AND 
CAST(REGEXP_REPLACE(duration, '[^0-9]', '', 'g') AS INTEGER) = 
(SELECT MAX(CAST(REGEXP_REPLACE(duration, '[^0-9]', '', 'g')AS INTEGER))
FROM NETFLIX 
);

/* 6. Finding content added in the last 5 years.
release_year as an INT Data type, and CURRENT_DATE is a DATE data type */

SELECT 
    *
FROM NETFLIX
where release_year >= EXTRACT(YEAR FROM CURRENT_DATE - INTERVAL '5 YEAR');

-- Another approach where we reference the date_added column

SELECT 
    *
FROM NETFLIX
where EXTRACT(YEAR FROM TO_DATE(date_added, 'Month DD,YYYY')) >= EXTRACT(YEAR FROM CURRENT_DATE - INTERVAL '5 YEAR');

-- 7. Finding all the movies/TV shows directed by Funke Akindele

SELECT 
    *
FROM netflix
where director = 'Funke Akindele' --the output is only "Your Excellency" but he is a co director for 'Omo Ghetto: the Saga' too

--  LIKE '% %'
SELECT 
    *
FROM netflix
where director ILIKE '%Funke Akindele%' --counts the rows where the name starts with a lowercase letter as well

-- 8. Listing all the the TV/SHOWS with more than 5 seasons

SELECT 
    *
FROM netflix
WHERE
type='TV Show' AND
SPLIT_PART(duration, ' ', 1)::INT > 5; -- output [5 seasons], where [5] is 1, thats what 1 returns, only the number

-- 9. Counting the number of content items in each genre

SELECT 
    TRIM(UNNEST(STRING_TO_ARRAY(listed_in, ','))) as genre,
    COUNT(*)
FROM netflix
GROUP BY 1
ORDER BY 2 DESC;

/*10. Finding each year the average number of content added by United States on netflix.
Returning Top 5 years with the highest average content release*/


WITH us_netflix as (
 SELECT 
        EXTRACT(YEAR FROM TO_DATE(date_added, 'Month DD, YYYY')) AS year,
        TRIM(UNNEST(STRING_TO_ARRAY(country, ','))) AS country
    FROM netflix
),
cte1 AS (
    SELECT
    year,
    COUNT(*) as total_content_per_year,
    ROUND(COUNT(*)::NUMERIC/(SELECT COUNT(*) FROM us_netflix WHERE country='United States')::NUMERIC * 100,2) as average_content_per_year
    FROM us_netflix
    WHERE country='United States'
    GROUP BY year
)
SELECT 
    *,
    RANK() OVER(ORDER BY average_content_per_year DESC) as rank
    FROM cte1
    ORDER BY rank
    limit 5;

-- 11. Listing all movies that are documentaries
 
SELECT * FROM netflix
where listed_in ILIKE '%Documentaries%';

-- 12. Findign all the content without a director

SELECT * FROM netflix
WHERE director IS NULL;

-- 13. Finding how many movies actor 'Naomi Higgins' appeared in the last 10 years

SELECT * FROM netflix
where "cast" ILIKE '%Naomi Higgins%'
AND
EXTRACT(YEAR FROM TO_DATE(date_added, 'Month DD, YYYY')) > EXTRACT (YEAR FROM CURRENT_DATE) - 10;


-- 14. Finding the top 10 actors who have appeared in the highest number of movies produced in United States

with us_actors as 
(
SELECT
    UNNEST(STRING_TO_ARRAY("cast", ',')) as actors,
    UNNEST(STRING_TO_ARRAY("country", ',')) as country,
    COUNT(*) as total_count
FROM netflix
GROUP BY 1,2
ORDER BY 2 DESC
)
SELECT 
    actors,
    total_count,
    ROW_NUMBER() OVER(ORDER BY total_count DESC)
    FROM us_actors
    WHERE country = 'United States' AND 
    actors IS NOT NULL AND 
    actors <> '' -- removes empty strings that may result from splitting
    LIMIT 10;

/* 15. Categorizing the content based on the presence of the keywords 'kill' and 'violence' in the description field.
Labelling content containing these keywords as 'Bad' and all other content as 'Good'. Counting how many items will fall into each category */

with content_cat AS (
SELECT *, 
CASE WHEN
    description ILIKE '%kill%'
    OR
    description ILIKE '%violence%' THEN 'Mature/Violence'
    ELSE 'Safe/General'
    END AS content_category
from netflix
)
SELECT 
    content_category,
    count(*)
    FROM content_cat
    GROUP BY 1;
    
/* 16. Creating a function that:
Takes country, content type (Movie/TV Show), and a year range
Returns:
-Total number of titles
-Average movie duration (minutes)
-Most common rating
-First and last year of release in that subset */

CREATE OR REPLACE FUNCTION analyze_content_by_country
(
    p_country VARCHAR(255),
    p_type VARCHAR(20),
    p_start_year INT,
    p_end_year INT
)
RETURNS TABLE (
    total_titles INT,
    avg_duration_minutes INT,
    most_common_rating VARCHAR(10),
    first_release_year INT,
    last_release_year INT
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    WITH filtered AS (
        SELECT
            release_year,
            rating,
            CAST(REGEXP_REPLACE(duration, '[^0-9]', '', 'g') AS INT) AS duration_minutes
        FROM netflix
        WHERE country = p_country
          AND type = p_type
          AND release_year BETWEEN p_start_year AND p_end_year
          AND duration ILIKE '%min%'
    ),
    rating_ranked AS (
        SELECT 
            rating,
            COUNT(*)
        FROM filtered
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 1
    )
    SELECT
        COUNT(*)::INT AS total_titles,            
        ROUND(AVG(duration_minutes))::INT,  
        (SELECT rating::VARCHAR(10) FROM rating_ranked),
        MIN(release_year)::INT ,
        MAX(release_year)::INT
    FROM filtered;
END;
$$;


-- testing the function

SELECT * FROM analyze_content_by_country(
    'United States',
    'Movie',
    2015,
    2022
)

/* 17. Creating a function that accepts:
-A keyword --somewhere in the description
-A minimum release year
-A minimum maturity rating (TV-MA)
Returns top N most relevant titles. Orders results by:
-Keyword frequency in description
-Newest content first */

CREATE OR REPLACE FUNCTION recommended_production
(
    p_keyword TEXT,
    p_minimum_release_year INT,
    p_min_rating VARCHAR(10),
    p_limit INT DEFAULT 10 -- Returns top N most relevant titles.
)
RETURNS TABLE(
    title VARCHAR(255),
    type VARCHAR(20),
    release_year INT, --newest content first
    rating VARCHAR(10),
    relevance_score NUMERIC
    --keyword frequence
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT  
        --Always make the SELECT column order match RETURNS TABLE exactly
        netflix.title,                        -- matches first RETURNS TABLE column
        netflix.type,                         -- matches second column
        netflix.release_year,                 -- third
        netflix.rating,                       -- fourth
        (
            LENGTH(LOWER(description)) - LENGTH(REPLACE(LOWER(description), LOWER(p_keyword), ''))
        )::NUMERIC /LENGTH(LOWER(p_keyword)) AS relevance_score
        FROM netflix
        WHERE 
        description ILIKE '%' || p_keyword || '%'
        AND netflix.release_year >= p_minimum_release_year
        AND netflix.rating IN ( 'TV-MA' , 'NC-17' , 'R' , 'NR' )
        ORDER BY relevance_score DESC, netflix.release_year DESC
        LIMIT p_limit;
END;
$$;

SELECT * 
FROM recommended_production(
    'crime'::TEXT,  -- p_keyword
    2001,           -- p_minimum_release_year
    'TV-MA'::VARCHAR, -- p_min_rating
    10               -- p_limit (optional)
);


DROP FUNCTION IF EXISTS recommended_production(
    TEXT,
    INT,
    VARCHAR,
    INT
);


/*
SELECT
    (LENGTH(LOWER(description)) - length(REPLACE(LOWER(DESCRIPTION), LOWER('Kirsten'), '')))
    /LENGTH('Kirsten') AS relevance_score
from netflix
where show_id='s1'

if the output is 1, it means that word has been mentioned 1 time
if the output is 2, it means the word has been mentioned 2 times


Verify the output type
SELECT pg_typeof(
    (LENGTH(LOWER(description)) - LENGTH(REPLACE(LOWER(description), LOWER('Kirsten'), '')))
    / LENGTH('Kirsten')
) AS column_type
FROM netflix
WHERE show_id='s1';

*/

