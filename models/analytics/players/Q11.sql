WITH PlayerCareerEvolution AS (
    SELECT
        f.player_name,
        f.r_age,
        AVG(f.r_points_per_game) AS avg_ppg, -- Scoring
        AVG(f.r_assists_per_game) AS avg_apg, -- Playmaking
        AVG(f.r_blocks_per_game + f.r_steals_per_game) AS avg_def_stocks -- Defensive Anchoring (Blocks + Steals)
    FROM {{ ref('fct__regular_rookie_playoffs_mvps_full_players_stats_seasons_97_22') }} f
    WHERE f.r_games >= 41
    GROUP BY 1, 2
)

SELECT
    player_name,
    r_age,
    avg_ppg,
    avg_apg,
    avg_def_stocks,
    -- Analyze change year-over-year using LAG function
    avg_ppg - LAG(avg_ppg, 1, avg_ppg) OVER (PARTITION BY player_name ORDER BY r_age) AS ppg_change,
    avg_apg - LAG(avg_apg, 1, avg_apg) OVER (PARTITION BY player_name ORDER BY r_age) AS apg_change
FROM PlayerCareerEvolution
-- Focusing on a few select players known for evolution (you'll need to filter/limit the final result in practice)