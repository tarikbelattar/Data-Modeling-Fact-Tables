-- DDL Query by Tarik Bel Attar
-- Date: 2024-12-03
-- Homework Task: A DDL for a user_devices_cumulated table that has:
-- - A device_activity_datelist tracking user activity days by browser_type.
-- - Device type (or browser type) represented as a column with multiple rows per user.

-- Step 1: Create the user_devices_cumulated table
CREATE TABLE user_devices_cumulated (
    user_id NUMERIC, -- The unique identifier for the user (supports large numbers due to NUMERIC type)
    device_type TEXT, -- The type of device (e.g., 'Chrome', 'Safari', 'Mobile')
    device_activity_datelist DATE[], -- An array of dates representing the days the user was active on this device type
    date DATE, -- The specific date this entry was logged or aggregated
    PRIMARY KEY (user_id, date, device_type) -- Ensure uniqueness for each user, date, and device type
);