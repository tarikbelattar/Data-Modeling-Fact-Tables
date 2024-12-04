
WITH yesterday AS(
    SELECT *
    FROM user_devices_cumulated
    WHERE date = DATE('2022-12-31')
),
  Today AS(
    SELECT
        e.user_id,
        d.device_type,
        array_agg(DISTINCT (DATE(e.event_time))) AS device_activity_list
    FROM events e
    INNER JOIN devices d on e.device_id = d.device_id
    WHERE DATE(e.event_time) = DATE('2023-01-01')
    AND e.user_id IS NOT NULL
    GROUP BY e.user_id, d.device_type
    )
INSERT INTO user_devices_cumulated
SELECT
    COALESCE(y.user_id, t.user_id) AS user_id,
    COALESCE(y.device_type, t.device_type) AS device_type,
    CASE
        WHEN y.device_activity_datelist IS NULL THEN t.device_activity_list
        WHEN y.device_activity_datelist IS NOT NULL THEN y.device_activity_datelist || t.device_activity_list
        ELSE y.device_activity_datelist
    END AS device_activity_list
FROM yesterday y
FULL OUTER JOIN today t
ON y.user_id = t.user_id