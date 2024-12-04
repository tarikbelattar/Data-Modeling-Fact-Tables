WITH dedupicated_game_details AS (SELECT g.game_date_est,
                                         *,
                                         row_number() over (partition by gd.game_id, gd.player_id) AS rn
                                  FROM game_details gd
                                  INNER JOIN public.games g on g.game_id = gd.game_id
                                  )
SELECT *
FROM dedupicated_game_details
WHERE rn = 1