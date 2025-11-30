WITH PerformanceDelta AS (
    SELECT
        f.player_name,
        f.seasons,
        f.p_player_efficiency_rating AS playoff_per,
        -- Assuming Regular Season PER is available as r_player_efficiency_rating, 
        -- but using an imputed stat based on available columns for demonstration.
        -- We will use the R_POINTS_PER_GAME as the metric for regular season performance.
        f.r_points_per_game, 
        f.p_points AS playoff_total_points, -- Total points needed for a fair comparison of magnitude
        f.r_games,
        f.p_playoff_games_in_season,
        
        -- Create a simple Playoff/Regular Season Score Delta (Playoff Points per Game - Regular Season Points per Game)
        (f.p_points / NULLIF(f.p_playoff_games_in_season, 0)) - f.r_points_per_game AS ppg_delta
    
    FROM {{ ref('fct__regular_rookie_playoffs_mvps_full_players_stats_seasons_97_22') }} f
    WHERE f.p_playoff_games_in_season > 5 -- Minimum playoff games threshold for meaningful comparison
      AND f.r_games > 41 -- Minimum regular season games threshold
)

SELECT
    player_name,
    AVG(ppg_delta) AS average_ppg_elevation,
    COUNT(seasons) AS seasons_analyzed
FROM PerformanceDelta
GROUP BY 1
ORDER BY average_ppg_elevation DESC
LIMIT 20;
