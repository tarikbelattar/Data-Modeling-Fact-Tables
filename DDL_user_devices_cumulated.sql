CREATE TABLE user_devices_cumulated(
    user_id BIGINT,
    device_type text,
    device_activity_datelist DATE[],
    date DATE,
    primary key (user_id, date)
)
-- DROP TABLE user_devices_cumulated