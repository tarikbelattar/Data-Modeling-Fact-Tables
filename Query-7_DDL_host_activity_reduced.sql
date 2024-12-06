-- Create the table host_activity_reduced to store reduced and aggregated host activity data
CREATE TABLE host_activity_reduced(
     host TEXT, -- The host identifier (e.g., website, server, or endpoint) for which activity is tracked
     month_start DATE, -- The starting date of the month (used for grouping data by month)
     hit_array INT[], -- An array storing the daily hit counts for the host in the corresponding month
     unique_visitors INT[], -- An array storing the daily count of unique visitors for the host in the corresponding month
    PRIMARY KEY (host, month_start) -- Ensures each (host, month_start) combination is unique
);