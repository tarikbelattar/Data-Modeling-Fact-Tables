-- Procedure: run_cumulative_hosts_pipeline
-- Purpose: This procedure is used to incrementally update the cumulative table `hosts_cumulated`
--          with daily host activity data from the `events` table.
-- Author: Tarik Bel Attar
-- Date: 2024-12-06

CREATE OR REPLACE FUNCTION run_cumulative_hosts_pipeline()
RETURNS void LANGUAGE plpgsql AS $pipeline$
DECLARE
    -- Declare variables for yesterday's and today's dates
    yesterday_date DATE := (SELECT MAX(date) FROM hosts_cumulated); -- Fetch the most recent date from the cumulative table
    today_date DATE := yesterday_date + INTERVAL '1 day'; -- Calculate today's date as one day after the most recent date
BEGIN
    -- Step 1: Retrieve cumulative data from yesterday
    WITH yesterday AS (
        SELECT *
        FROM hosts_cumulated
        WHERE date = yesterday_date -- Filter cumulative data for the most recent date
    ),

    -- Step 2: Aggregate today's host activity data
    Today AS (
        SELECT
            e.host, -- The host identifier (e.g., website, server, or endpoint)
            array_agg(DISTINCT DATE(e.event_time)) AS host_activity_list
            -- Aggregate the distinct dates of events for each host
        FROM events e
        WHERE DATE(e.event_time) = today_date -- Filter events that occurred today
          AND e.user_id IS NOT NULL -- Exclude events with null user IDs
        GROUP BY e.host -- Group by host to aggregate activity for each host
    )

    -- Step 3: Insert or update the cumulative table
    INSERT INTO hosts_cumulated
    SELECT DISTINCT
        COALESCE(y.host, t.host) AS host, -- Use the host from "yesterday" if available; otherwise, use today's host
        CASE
            WHEN y.host_activity_datelist IS NULL THEN t.host_activity_list
            -- If the host is new, use today's activity list
            WHEN y.host_activity_datelist IS NOT NULL THEN y.host_activity_datelist || t.host_activity_list
            -- If the host already exists, append today's activity list to yesterday's list
            ELSE y.host_activity_datelist -- Handle edge cases where neither yesterday nor today has valid data
        END AS host_activity_datelist, -- The updated cumulative activity list
        today_date AS date -- Set the date for today's cumulative record
    FROM yesterday y
    FULL OUTER JOIN today t
    ON y.host = t.host; -- Match yesterday's and today's data by host

    -- Step 4: Log a message to indicate successful execution
    RAISE NOTICE 'Cumulative pipeline ran for %', today_date;

END;
$pipeline$;