-- Query by Tarik Bel Attar
-- Date: 2024-12-04
-- Homework Question: Construct a query to deduplicate game_details from Day 1 so there are no duplicates.

-- Step 1: Create a CTE to deduplicate game_details
WITH dedupicated_game_details AS (
    SELECT 
        g.game_date_est, -- The game date in EST timezone from the games table
        *, -- Select all columns from game_details
        row_number() OVER (PARTITION BY gd.game_id, gd.player_id) AS rn
        -- Assign a unique row number to each combination of game_id and player_id
        -- Duplicates (if any) will have rn > 1
    FROM 
        game_details gd -- The game_details table containing detailed stats for each game
    INNER JOIN 
        public.games g -- Join with the games table to get additional game metadata
    ON 
        g.game_id = gd.game_id -- Match records using the game_id column
)

-- Step 2: Select deduplicated rows
SELECT *
FROM dedupicated_game_details
WHERE rn = 1; -- Filter to include only the first occurrence of each game_id/player_id combination