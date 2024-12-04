CREATE TABLE user_devices_cumulated(
    user_id NUMERIC,
    device_type text,
    device_activity_datelist DATE[],
    date DATE,
    primary key (user_id, date, device_type)
)
--DROP TABLE user_devices_cumulated