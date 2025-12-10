-- 1️⃣1️⃣ How do player roles evolve — scorers, playmakers, defensive anchors — across their careers?
with player_stats as (
    select *
    from {{ ref('fct__regular_rookie_playoffs_mvps_full_players_stats_seasons_97_22') }}
) 

,playercareerevolution as (
    select
        f.seasons,
        f.player_name,
        f.r_age,
        f.r_points_per_game as avg_ppg,-- Scoring
        f.r_assists_per_game as avg_apg,-- Playmaking
        (f.r_blocks_per_game + f.r_steals_per_game) as avg_def_stocks-- Defensive Anchoring (Blocks + Steals)
    from player_stats f
    where f.r_games >= 41
    
)

select
    seasons,
    player_name,
    r_age,
    avg_ppg,
    avg_apg,
    avg_def_stocks,
    -- Analyze change year-over-year using LAG function
    avg_ppg - lag(avg_ppg, 1, avg_ppg) over (partition by player_name order by r_age) as ppg_change,
    avg_apg - lag(avg_apg, 1, avg_apg) over (partition by player_name order by r_age) as apg_change
from playercareerevolution
-- Focusing on a few select players known for evolution (you'll need to filter/limit the final result in practice)