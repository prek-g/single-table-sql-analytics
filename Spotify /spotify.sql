CREATE DATABASE spotify_db

DROP TABLE IF EXISTS spotify;
CREATE TABLE spotify (
    artist VARCHAR(255),
    track VARCHAR(255),
    album VARCHAR(255),
    album_type VARCHAR(50),
    danceability FLOAT,
    energy FLOAT,
    loudness FLOAT,
    speechiness FLOAT,
    acousticness FLOAT,
    instrumentalness FLOAT,
    liveness FLOAT,
    valence FLOAT,
    tempo FLOAT,
    duration_min FLOAT,
    title VARCHAR(255),
    channel VARCHAR(255),
    views FLOAT,
    likes BIGINT,
    comments BIGINT,
    licensed BOOLEAN,
    official_video BOOLEAN,
    stream BIGINT,
    energy_liveness FLOAT,
    most_played_on VARCHAR(50)
);
-- psql :\copy spotify FROM '/mnt/c/Users/gjoni/Desktop/Spotify_SQL/spotify.csv' DELIMITER ',' CSV HEADER;

SELECT * FROM spotify

-- 1. Finding tracks with the highest audience engagement, where engagement is a combination of likes, comments, and streams.

SELECT
    track,
    artist,
    stream,
    likes,
    comments,
    ROUND((COALESCE(likes,0) + COALESCE(comments,0)) / stream::NUMERIC, 6) AS engagement_ratio
FROM spotify
WHERE stream IS NOT NULL AND stream <> 0
ORDER BY engagement_ratio DESC
LIMIT 20;

-- 2. Getting the total number of comments for tracks depending on the license

SELECT
    SUM(CASE WHEN licensed=TRUE THEN comments ELSE 0 END) AS total_licensed_tracks_comments,
    SUM(CASE WHEN licensed=FALSE THEN comments ELSE 0 END) AS total_unlicensed_tracks_comments
FROM spotify;

-- 3. Calculating average danceability, energy, and valence for each album, separated by platform (most_played_on) for albums that have at least 3 tracks

SELECT
    album,
    most_played_on,
    COUNT(*) AS track_count,
    ROUND(AVG(danceability)::NUMERIC, 2) AS avg_danceability,
    ROUND(AVG(energy)::NUMERIC, 2) AS avg_energy,
    ROUND(AVG(valence)::NUMERIC, 2) AS avg_valence
FROM spotify
GROUP BY album, most_played_on
HAVING COUNT(*) >= 3 
ORDER BY avg_energy DESC;

-- 4. Finding all tracks that belong to the album type single with more than 1 million streams

SELECT 
    track,
    album_type,
    stream,
    likes
FROM spotify
WHERE album_type ILIKE 'Single'
  AND stream > 1000000
ORDER BY stream DESC;

-- 5. Counting the total number of tracks by each artist

SELECT 
    artist,
    COUNT(track) AS total_tracks_per_artist
FROM SPOTIFY
GROUP BY 1
ORDER BY 2 ASC

-- 6. Calculating the average danceability of tracks in each album

SELECT 
    album,
    ROUND(AVG(danceability)::NUMERIC, 2) as avg_danceability --round does not work on FLOAT
FROM spotify
GROUP BY 1
ORDER BY 2 DESC

/* 7. Finding the top 5 artists with the highest average engagement (likes+comments)/streams WHERE:
- I only consider tracks with stream > 0 and more than 3 tracks
- Return artist, total tracks &  avg_engagement_ratio */ 

-- First Approach using Subqueries
SELECT
    artist,
    count(track) AS total_tracks,
    ROUND(AVG(avg_engagement_ratio)::NUMERIC,2) as avg_engagement_ratio
FROM (
SELECT
    artist,
    track,
    (COALESCE(likes,0) + COALESCE(comments,0))::NUMERIC/stream::NUMERIC AS avg_engagement_ratio
FROM spotify
WHERE stream IS NOT NULL 
AND 
stream>0
) AS t1
GROUP BY 1
HAVING count(track)>3
ORDER BY 3 DESC 
LIMIT 5;

-- Second approach using CTEs

WITH CTE1 AS 
( 
SELECT
    artist,
    track,
    (COALESCE(likes,0) + COALESCE(comments,0))::NUMERIC/stream::NUMERIC AS engagement_ratio
FROM spotify
WHERE stream IS NOT NULL AND stream>0
),
CTE2 AS
(
SELECT
    artist,
    COUNT(track) AS total_tracks,
    ROUND(AVG(engagement_ratio)::NUMERIC,2) as avg_engagement_ratio
FROM CTE1
GROUP BY 1
)
SELECT * 
FROM CTE2
WHERE total_tracks > 3
ORDER BY 3 DESC
LIMIT 5

-- Result is the same from both cte method and subquery method.

-- 8. Listing all the tracks along with their views and likes depending where official_video = True

select
    track,
    SUM(likes) as total_likes,
    SUM(views) as total_views
FROM spotify
WHERE official_video = true
GROUP BY 1
ORDER BY 2 DESC,3 DESC

-- 9. For each album, calculating the total views and all associated tracks.

SELECT
    album,
    track,
    SUM(views) as total_views
FROM SPOTIFY
GROUP BY 1,2
ORDER BY 3 DESC

-- 10. Retrieving the track names along with their streams based where streams on spotify are greater than on youtube

-- Query below returns for us the songs that appear on both most_played_on Youtube AND Spotify
SELECT track
FROM spotify
WHERE most_played_on IN ('Youtube', 'Spotify')
GROUP BY track
HAVING COUNT(DISTINCT most_played_on) = 2;

-- Solution

SELECT * FROM 
(
SELECT 
    track,
    --most_played_on,
    COALESCE(SUM(CASE WHEN most_played_on = 'Youtube' THEN stream END),0) as streamed_on_youtube,
    COALESCE(SUM(CASE WHEN most_played_on = 'Spotify' THEN stream END),0) as streamed_on_spotify 
FROM spotify
GROUP BY 1
) AS t1
WHERE 
    streamed_on_spotify > streamed_on_youtube
    AND streamed_on_youtube <> 0
ORDER BY 3 DESC

-- 11. Finding top 3 most viewed tracks for each artist

SELECT * FROM (
SELECT
    artist,
    track,
    SUM(views) as total_views,
    DENSE_RANK() OVER(PARTITION BY artist ORDER BY SUM(views) DESC )AS rank
FROM spotify
GROUP BY 1,2
order by 1 ,3 desc
) AS t1
WHERE RANK <=3

-- 12. Writing a query to find tracks where the liveness score is above the average

-- weak approach
SELECT AVG(liveness) from spotify -- 0.19365327765368573

SELECT
    track,
    liveness
FROM spotify
WHERE liveness > 0.19365327765368573

-- the query above isnt accurate and reproducible, because the dataset may change and new values will be added, avg changes

-- correct approach
SELECT
    track,
    liveness
FROM spotify
WHERE liveness > (SELECT AVG(liveness) from spotify )

-- 13. Calculating the difference between the highest and lowest energy values for tracks in each album. 

-- First approach, with a subquery
WITH ene AS
(
SELECT
    album,
    MAX(energy) as max_energy_track,
    MIN(energy) as min_energy_track
FROM spotify
GROUP BY 1
)
SELECT
    album,
    max_energy_track - min_energy_track AS ene_difference
FROM ene
ORDER BY 2 DESC

/*  14. Creating a SQL function that classifies a track's "engagement quality" based on normalized engagement metrics.
Streaming platforms often need a single engagement score instead of raw likes/views/comments.
I will:
- Normalize likes and comments by views
- Weight them
- Return a score category (Low / Medium / High)     */

/*Normalization = making values comparable, like_rate = likes/views | comment_rate = comments/views. Then i weight them giving likes more value ( a higher credit) and giving comments a lower credit. 
engagement_score = like_rate * like_credit + comment_rate * comment_credit. Depending on the result they will be classified as Low, Medium, or High. */

CREATE OR REPLACE FUNCTION get_engagement_tier 
(
    p_likes NUMERIC,
    p_comments NUMERIC,
    p_views NUMERIC
)

RETURNS TEXT
LANGUAGE plpgsql
AS $$

DECLARE
    comment_rate NUMERIC;
    like_rate NUMERIC;
    engagement_score NUMERIC;
BEGIN
    -- since there will be divisions I will protect division by zero
    if p_views IS NULL or p_views = 0 THEN
    RETURN 'Unknown';
    END IF;

    -- Normalizing likes and comments by views
    like_rate := p_likes/p_views;                             -- := is assigning, borrowed by Pascal-style languages, while = is a comparison
    comment_rate := p_comments/p_views;

    -- weighting the score, giving a 70% credit to likes (more important than comments) and 30% credit to comments.
    engagement_score := (like_rate * 0.7) + (comment_rate * 0.3);

    /* classifying them based on the engagement score as Low, Medium and High

    select avg(views) from spotify     92037403.61178014
    select avg(likes) from spotify     647990.153782655142
    select avg(comments) from spotify  26846.789744585802
    Based on this avg_like_rate = select 647990.153782655142/92037403.61178014  = 0.00704050884047012558
                  avg_comment_rate = select 26846.789744585802/92037403.61178014 = 0.00029169434046431099
                  engagement_score = select ( 0.00704050884047012558 * 0.7) + (0.00029169434046431099 * 0.3) = 0.005015864490468381203

    The avg engagement score is 0.005 */

    IF engagement_score >= 0.006 THEN 
        RETURN 'High';

    ELSIF engagement_score >= 0.002 THEN
        RETURN 'Medium';

    ELSE
        RETURN 'Low';
    END IF;
END;
$$;


-- Testing the functioN

SELECT likes, views, comments FROM spotify

SELECT get_engagement_tier(
    6220896,
    169907,
    72011645
) -- High

SELECT get_engagement_tier(
    3117787,
    24407,
    621765645
) -- Medium

SELECT get_engagement_tier(
    18,
    1,
    524
)  -- High


SELECT get_engagement_tier(
    3497228,
    34400,
    1665814269
)  -- Low

ALTER TABLE spotify
ADD COLUMN IF NOT EXISTS engagement_ratio NUMERIC;

SELECT * FROM spotify

/*Creating a stored procedure that 
- Takes a track name as input
- Checks if engagement_ratio has already been calculated
- if not calculates and updates it
- optionally prints a raise notice 
*/

CREATE OR REPLACE PROCEDURE update_track_engagement_ratio (p_track VARCHAR, p_artist VARCHAR)
LANGUAGE plpgsql
AS $$
DECLARE
    v_ratio NUMERIC;
BEGIN
    -- I want to calculate the engagement_ratio of a track, first i need to make sure that a track exists
    SELECT (COALESCE(likes,0) + COALESCE(comments,0))::NUMERIC / stream::NUMERIC  
    INTO
    v_ratio
    FROM spotify
    WHERE track= p_track AND
    stream IS NOT NULL AND stream>0;

    IF FOUND THEN
        -- Updating engagement_ratio column
        UPDATE spotify
        set engagement_ratio = v_ratio
        WHERE track = p_track;

        RAISE NOTICE 'Engagement ratio updated for track:% by artist:%', p_track, p_artist;
    ELSE
        RAISE NOTICE '%  by % not eligible for update (stream=0 or not found)', p_track, p_artist;
    END IF;
END;
$$;

CALL update_track_engagement_ratio('Feel Good Inc.', 'Gorillaz')

SELECT * FROM spotify
where track = 'Feel Good Inc.'

