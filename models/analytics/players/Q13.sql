-- 1️⃣3️⃣ What stats most strongly predict winning the season MVP?

WITH MVPStats AS (
    SELECT
        rsm.rsm_player_name,
        rsm.rsm_points_per_game,
        rsm.rsm_assists_per_game,
        rsm.rsm_total_rebounds_per_game,
        rsm.rsm_win_share_per_48_games
    FROM {{ ref('fct__regular_rookie_playoffs_mvps_full_players_stats_seasons_97_22') }} rsm
    WHERE rsm.rsm_player_name IS NOT NULL -- Player received MVP votes (or was the MVP)
),

LeagueAvgStats AS (
    SELECT
        AVG(r.r_points_per_game) AS league_avg_ppg,
        AVG(r.r_assists_per_game) AS league_avg_apg,
        AVG(r.r_total_rebounds_per_game) AS league_avg_rpg
    FROM {{ ref('fct__regular_rookie_playoffs_mvps_full_players_stats_seasons_97_22') }} r
    -- Filter to get a representative sample of regular season player performance
    WHERE r.r_games >= 41 AND r.r_minutes_played_per_game >= 20
)

SELECT
    'MVP Players' AS category,
    AVG(rsm_points_per_game) AS avg_ppg,
    AVG(rsm_assists_per_game) AS avg_apg,
    AVG(rsm_total_rebounds_per_game) AS avg_rpg
FROM MVPStats
UNION ALL
SELECT
    'League Average Players' AS category,
    league_avg_ppg,
    league_avg_apg,
    league_avg_rpg
FROM LeagueAvgStats
