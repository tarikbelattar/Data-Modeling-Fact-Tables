-- Cumulative Query by Tarik Bel Attar
-- Date: 2024-12-04
-- Homework Task: A cumulative query to generate device_activity_datelist from events.

-- Step 1: Retrieve yesterday's data
WITH yesterday AS (
    SELECT *
    FROM user_devices_cumulated
    WHERE date = DATE('2023-01-11') -- Select the cumulative data for the previous day (January 11, 2023)
),

-- Step 2: Retrieve today's data
Today AS (
    SELECT
        e.user_id, -- The user's ID from the events table
        d.device_type, -- The type of device from the devices table
        array_agg(DISTINCT DATE(e.event_time)) AS device_activity_list
        -- Aggregate the distinct dates of events for each user and device type
    FROM events e
    INNER JOIN devices d ON e.device_id = d.device_id
    -- Join events with devices to retrieve the device type
    WHERE DATE(e.event_time) = DATE('2023-01-12') -- Filter events that occurred today (January 12, 2023)
      AND e.user_id IS NOT NULL -- Exclude events with null user IDs
    GROUP BY e.user_id, d.device_type -- Group by user and device type to aggregate activity by device
)

-- Step 3: Insert the cumulative data
INSERT INTO user_devices_cumulated
SELECT DISTINCT
    COALESCE(y.user_id, t.user_id) AS user_id, -- Use user_id from "yesterday" if available, otherwise from "today"
    COALESCE(y.device_type, t.device_type) AS device_type, -- Use device_type from "yesterday" or "today"
    CASE
        WHEN y.device_activity_datelist IS NULL THEN t.device_activity_list
        -- If the user or device type is new, use today's activity list
        WHEN y.device_activity_datelist IS NOT NULL THEN y.device_activity_datelist || t.device_activity_list
        -- If the user or device type already exists, append today's activity list to yesterday's list
        ELSE y.device_activity_datelist -- Handle edge cases where neither yesterday nor today has valid data
    END AS device_activity_list, -- The updated cumulative activity list
    DATE('2023-01-12') AS date -- Set the date for today's cumulative record
FROM yesterday y
FULL OUTER JOIN today t
ON y.user_id = t.user_id -- Match users between yesterday's and today's data
AND y.device_type = t.device_type; -- Match device types between yesterday's and today's data