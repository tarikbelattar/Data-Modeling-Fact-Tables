-- Homework Data modeling Fact tables
-- Query n°4
-- A datelist_int generation query. Convert the device_activity_datelist column into a datelist_int column
-----------------------Tarik BEL ATTAR------------------------------------------------------------------
-- 2024-12-04
-- Step 1: Define the "hosts" CTE (Common Table Expression)
-- This step retrieves all user activity data from the "user_devices_cumulated" table for a specific date ('2023-01-05').
WITH hosts AS (
    SELECT *
    FROM user_devices_cumulated
    WHERE date = '2023-01-05'
),

-- Step 2: Define the "series" CTE
-- This generates a series of dates from '2023-01-01' to '2023-01-31', with an interval of 1 day between each date.
-- The dates are converted into a proper DATE format and aliased as "series_date".
series AS (
    SELECT DATE(series_dates) AS series_date
    FROM generate_series('2023-01-01', '2023-01-31', INTERVAL '1 day') AS series_dates
),

-- Step 3: Define the "placeholder_datelist_ints" CTE
-- This CTE calculates an integer representation of user activity for each date in the series.
placeholder_datelist_ints AS (
    SELECT
        user_id, -- The user's ID
        h.date - DATE(s.series_date), -- The difference in days between the activity date (h.date) and the generated date (series_date)
        CASE
            -- Check if the current series_date is in the user's activity datelist
            WHEN h.device_activity_datelist @> ARRAY[s.series_date]
            THEN
                -- If the user was active on the series_date, calculate the corresponding bit position
                -- using the formula: 2^(31 - (h.date - series_date)).
                CAST(POW(2, 31 - (h.date - DATE(s.series_date))) AS BIGINT)
            ELSE
                -- Otherwise, set the value to 0 (indicating no activity)
                0
        END AS datelist_int, -- The calculated bit integer for the given date
        h.date, -- The date of user activity from the "hosts" table
        s.series_date -- The generated date from the "series" CTE
    FROM hosts h
    CROSS JOIN series s -- Perform a CROSS JOIN to pair each user with every date in the series
)

-- Step 4: Final Query
-- Aggregate the data from "placeholder_datelist_ints" to compute a bit representation of user activity
SELECT
    user_id, -- The user's ID
    -- Sum up all calculated bit values (datelist_int) and cast the result into a 32-bit integer
    CAST(CAST(SUM(datelist_int) AS BIGINT) AS BIT(32)) AS datelist_bit_int,
    -- Count the number of active days (1 bits) in the 32-bit integer
    bit_count(CAST(CAST(SUM(datelist_int) AS BIGINT) AS BIT(32))) AS user_days_active
FROM placeholder_datelist_ints
GROUP BY user_id; -- Group the results by user ID to compute aggregates per user