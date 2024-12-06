CREATE OR REPLACE FUNCTION run_cumulative_pipeline()
RETURNS void LANGUAGE plpgsql AS $pipeline$
DECLARE
    yesterday_date DATE := (SELECT MAX(date) FROM user_devices_cumulated);
    today_date DATE := yesterday_date + INTERVAL '1 day';
BEGIN
    -- Execute the pipeline
    WITH yesterday AS (
        SELECT *
        FROM user_devices_cumulated
        WHERE date = yesterday_date
    ),
    today AS (
        SELECT
            e.user_id,
            d.device_type,
            ARRAY_AGG(DISTINCT DATE(e.event_time)) AS device_activity_list
        FROM events e
        INNER JOIN devices d ON e.device_id = d.device_id
        WHERE DATE(e.event_time) = today_date
          AND e.user_id IS NOT NULL
        GROUP BY e.user_id, d.device_type
    )
    INSERT INTO user_devices_cumulated (user_id, device_type, device_activity_datelist, date)
    SELECT DISTINCT
        COALESCE(y.user_id, t.user_id) AS user_id,
        COALESCE(y.device_type, t.device_type) AS device_type,
        CASE
            WHEN y.device_activity_datelist IS NULL THEN t.device_activity_list
            WHEN y.device_activity_datelist IS NOT NULL THEN y.device_activity_datelist || t.device_activity_list
            ELSE y.device_activity_datelist
        END AS device_activity_datelist,
        today_date AS date
    FROM yesterday y
    FULL OUTER JOIN today t
    ON y.user_id = t.user_id AND y.device_type = t.device_type;

    RAISE NOTICE 'Cumulative pipeline ran for %', today_date;
END;
$pipeline$;