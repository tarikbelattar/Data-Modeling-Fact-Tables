-- host_activity_reduced Query by Tarik Bel Attar
-- Date: 2024-12-06
-- Homework Task: incremental query that loads host_activity_reduced from events.
-- Step 1: Aggregate daily data for March 2023
WITH daily_aggregate AS (
    SELECT
          host, -- The host (website, server, or endpoint) for which data is being aggregated
          COUNT(1) AS hits, -- Total number of events (hits) for this host
          COUNT(DISTINCT user_id) AS unique_visitors, -- Count of unique users who visited the host
          TO_CHAR(DATE(event_time), 'YYYY-MM-01')::DATE AS year_month -- The first day of the month to group data by month
    FROM events -- Source table containing raw event data
    WHERE user_id IS NOT NULL -- Exclude records where the user ID is NULL
        AND TO_CHAR(DATE(event_time), 'YYYY-MM') = '2023-03' -- Filter for events in March 2023
    GROUP BY host, TO_CHAR(DATE(event_time), 'YYYY-MM-01') -- Group by host and the first day of the month
),

-- Step 2: Retrieve historical data for January 2023 from the reduced fact table
yesterday_array AS (
    SELECT *
    FROM host_activity_reduced -- Reduced fact table containing historical data
    WHERE TO_CHAR(DATE(month_start), 'YYYY-MM') = '2023-01' -- Filter for January 2023
)

-- Step 3: Insert or update data in the reduced fact table
INSERT INTO host_activity_reduced
SELECT
    COALESCE(d.host, y.host) AS host, -- Use the host from new data (daily_aggregate) or historical data (yesterday_array)
    COALESCE(y.month_start, d.year_month) AS month_start, -- Use the month_start from historical data or fallback to the new data
    CASE
        WHEN y.hit_array IS NOT NULL THEN y.hit_array || ARRAY[COALESCE(d.hits, 0)] -- Append new hits to the existing array
        WHEN y.hit_array IS NULL THEN array_fill(0, ARRAY[COALESCE(y.month_start - d.year_month, 0)]) || ARRAY[COALESCE(d.hits, 0)]
        ELSE y.hit_array -- Keep the existing array if there's no new data
    END AS hit_array, -- Final hit array for the month
    CASE
        WHEN y.unique_visitors IS NOT NULL THEN y.unique_visitors || ARRAY[COALESCE(d.unique_visitors, 0)] -- Append new unique visitors to the existing array
        WHEN y.unique_visitors IS NULL THEN array_fill(0, ARRAY[COALESCE(y.month_start - d.year_month, 0)]) || ARRAY[COALESCE(d.unique_visitors, 0)]
        ELSE y.unique_visitors -- Keep the existing array if there's no new data
    END AS unique_visitors -- Final unique visitors array for the month
FROM daily_aggregate d -- Daily aggregated data (new data for March 2023)
FULL OUTER JOIN yesterday_array y -- Combine new data with historical data
ON d.host = y.host -- Match records by host

-- Step 4: Resolve conflicts when inserting data
ON CONFLICT (host, month_start) -- Define conflict resolution for duplicate (host, month_start) keys
DO UPDATE SET
    hit_array = excluded.hit_array, -- Update hit_array with the new data
    unique_visitors = excluded.unique_visitors; -- Update unique_visitors with the new data
