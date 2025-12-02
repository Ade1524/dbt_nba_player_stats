-- 7️⃣ Which players elevate their performance the most in playoffs vs regular season?
with player_stats as (
    select *
    from {{ ref('fct__regular_rookie_playoffs_mvps_full_players_stats_seasons_97_22') }}
)

,performancedelta as (
    select
        f.player_name,
        f.seasons,
        f.p_player_efficiency_rating as playoff_per,
        f.r_points_per_game,
        f.p_points as total_points,
        f.r_games,
        f.p_playoff_games_in_season,
        (f.p_points / nullif(f.p_playoff_games_in_season, 0)) - f.r_points_per_game as ppg_delta
    from player_stats f
    where 
          f.p_playoff_games_in_season > 5
      and f.r_games > 41
)

select
    player_name,
    avg(ppg_delta) as average_ppg_elevation,
    count(seasons) as seasons_analyzed
from performancedelta
group by 
    player_name
order by average_ppg_elevation desc
