WITH PlayoffStats AS (
    SELECT
        f.player_name,
        CAST(SPLIT_PART(f.seasons, '-', 1) AS INTEGER) AS season_start_year,
        CAST(SPLIT_PART(f.seasons, '-', 2) AS INTEGER) AS season_end_year,
        f.p_player_efficiency_rating AS playoff_per,
        f.p_playoff_games_in_season
    FROM {{ ref('fct__regular_rookie_playoffs_mvps_full_players_stats_seasons_97_22') }} f
    WHERE f.p_playoff_games_in_season > 0
),

Rolling3YearAvg AS (
    SELECT
        player_name,
        season_start_year,
        -- Calculate 3-year rolling average PER
        AVG(playoff_per) OVER (
            PARTITION BY player_name
            ORDER BY season_start_year
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW -- Window of (Year-2), (Year-1), and Current Year
        ) AS rolling_3_year_playoff_per_avg
    FROM PlayoffStats
)

SELECT
    player_name,
    season_start_year AS ending_year_of_run,
    rolling_3_year_playoff_per_avg
FROM Rolling3YearAvg
WHERE rolling_3_year_playoff_per_avg IS NOT NULL
QUALIFY RANK() OVER (ORDER BY rolling_3_year_playoff_per_avg DESC) <= 20
ORDER BY rolling_3_year_playoff_per_avg DESC
