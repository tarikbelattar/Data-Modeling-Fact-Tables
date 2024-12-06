-- Create the table hosts_cumulated to store cumulative host activity data
CREATE TABLE hosts_cumulated(
    host TEXT, -- The host identifier (e.g., website) being tracked
    host_activity_datelist DATE[], -- An array of dates representing the days when the host was active
    date DATE, -- The date corresponding to the cumulative record (used for incremental updates)
    PRIMARY KEY(host, date) -- Ensures uniqueness for each (host, date) combination
);