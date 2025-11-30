WITH PlayerYearOverYear AS (
    SELECT
        f.player_name,
        f.dim_team_key,
        f.r_teams, -- Team Abbreviation
        f.seasons,
        CAST(SPLIT_PART(f.seasons, '-', 1) AS INTEGER) AS season_start_year,
        CAST(SPLIT_PART(f.seasons, '-', 2) AS INTEGER) AS season_end_year,
        f.p_win_share_per_48_games AS current_ws48,
        
        -- Get the WS/48 from the previous season for the same player
        LAG(f.p_win_share_per_48_games, 1) OVER (
            PARTITION BY f.player_name
            ORDER BY season_start_year  -- Order by the integer year
        ) AS previous_ws48,
        
        -- Get the team from the previous season
        LAG(f.r_teams, 1) OVER (
            PARTITION BY f.player_name
            ORDER BY season_start_year
        ) AS previous_team
        
    FROM {{ ref('fct__regular_rookie_playoffs_mvps_full_players_stats_seasons_97_22') }} f
    WHERE f.r_games >= 41 AND f.p_win_share_per_48_games IS NOT NULL
),

TeamDevelopment AS (
    SELECT
        p.r_teams AS current_team_abbr,
        t.team_name,
        p.current_ws48 - p.previous_ws48 AS ws48_improvement
    FROM PlayerYearOverYear p
    JOIN {{ ref('dim_nba_teams') }} t
        ON p.dim_team_key = t.dim_team_key-- Assuming dim_nba_teams has 'teams' column matching fct 'r_teams'
    -- Filter for cases where the player improved AND the team did not change
    WHERE p.current_ws48 > p.previous_ws48 
      AND p.r_teams = p.previous_team
)

SELECT
    team_name,
    AVG(ws48_improvement) AS avg_player_development_metric,
    COUNT(*) AS total_improvement_instances
FROM TeamDevelopment
GROUP BY 1
HAVING COUNT(*) > 10 -- Only look at teams with enough development instances
ORDER BY avg_player_development_metric DESC