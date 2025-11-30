WITH PlayerSeasons AS (
    SELECT
        f.player_name,
        CAST(SPLIT_PART(f.seasons, '-', 1) AS INTEGER) AS season_start_year,
        CAST(SPLIT_PART(f.seasons, '-', 2) AS INTEGER) AS season_end_year
        ,f.r_points_per_game
        ,f.p_player_efficiency_rating AS playoff_per
        ,f.r_age
    FROM {{ ref('fct__regular_rookie_playoffs_mvps_full_players_stats_seasons_97_22') }} f
    -- Filter to only use players with sufficient minutes/games played in the regular season
    WHERE f.r_games > 20 AND f.r_minutes_played_per_game > 15
),

PlayerEraStats AS (
    SELECT
        player_name,
        season_start_year,
        -- Define eras based on starting year
        CASE 
            WHEN season_start_year BETWEEN 1997 AND 2001 THEN '1997-2001 (Late 90s)'
            WHEN season_start_year BETWEEN 2002 AND 2006 THEN '2002-2006 (Early 00s)'
            WHEN season_start_year BETWEEN 2007 AND 2011 THEN '2007-2011 (Mid 00s)'
            WHEN season_start_year BETWEEN 2012 AND 2016 THEN '2012-2016 (Early 10s)'
            WHEN season_start_year BETWEEN 2017 AND 2021 THEN '2017-2021 (Modern Era)'
            ELSE 'Other' 
        END AS era_window,
        r_points_per_game
    FROM PlayerSeasons
    WHERE era_window != 'Other' -- Filter out incomplete eras
)

SELECT
    era_window,
    player_name,
    -- Calculate average PPG as a proxy for "best statistical player"
    AVG(r_points_per_game) AS avg_ppg_in_era,
    RANK() OVER (PARTITION BY era_window ORDER BY AVG(r_points_per_game) DESC) AS rank_in_era
FROM PlayerEraStats
GROUP BY 1, 2
ORDER BY era_window, rank_in_era
