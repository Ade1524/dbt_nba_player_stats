with PlayerCareerStats AS (
    SELECT
        player_name,
        r_points_per_game AS ppg,
        r_games
    FROM {{ ref('fct__regular_rookie_playoffs_mvps_full_players_stats_seasons_97_22') }}
    WHERE r_games >= 41 -- Filter for seasons with meaningful participation
),

PlayerVariance AS (
    SELECT
        player_name,
        AVG(ppg) AS mean_ppg,
        STDDEV(ppg) AS stddev_ppg,
        COUNT(ppg) AS total_seasons
    FROM PlayerCareerStats
    GROUP BY 1
    HAVING COUNT(ppg) >= 5 -- Only analyze players with at least 5 seasons
)

SELECT
    player_name,
    mean_ppg,
    stddev_ppg,
    total_seasons,
    -- Coefficient of Variation (CV) = Standard Deviation / Mean
    (stddev_ppg / NULLIF(mean_ppg, 0)) AS ppg_coefficient_of_variation
FROM PlayerVariance
ORDER BY ppg_coefficient_of_variation ASC, mean_ppg DESC -- Low CV is good, break ties with higher mean
-- LIMIT 20