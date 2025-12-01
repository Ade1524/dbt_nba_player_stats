-- 9️⃣ Who are the most consistent players year-over-year (low variance)?
with  player_stats as (
    select *
    from {{ ref('fct__regular_rookie_playoffs_mvps_full_players_stats_seasons_97_22') }}
)

,playercareerstats as (
    select
        player_name,
        r_points_per_game as ppg,
        r_games
    from player_stats
    where 
        r_games >= 41
)

,playervariance as (
    select
        player_name,
        avg(ppg) as mean_ppg,
        stddev(ppg) as stddev_ppg,
        count(ppg) as total_seasons
    from playercareerstats
    group by 
        player_name
    having 
        count(ppg) >= 5
)

select
    player_name,
    mean_ppg,
    stddev_ppg,
    total_seasons,
    (stddev_ppg / nullif(mean_ppg, 0)) as ppg_coefficient_of_variation
from playervariance
order by 
    ppg_coefficient_of_variation asc, 
    mean_ppg desc