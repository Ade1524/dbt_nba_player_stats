-- 6️⃣ Who are the best statistical players of each era (late 90s → today)?


with player_stats as (
    select *
    from {{ ref('fct__regular_rookie_playoffs_mvps_full_players_stats_seasons_97_22') }}
)

, playerseasons as (
    select
        f.player_name,
        cast(split_part(f.seasons, '-', 1) as integer) as season_start_year,
        cast(split_part(f.seasons, '-', 2) as integer) as season_end_year,
        f.r_points_per_game,
        f.p_player_efficiency_rating as playoff_per,
        f.r_age
    from player_stats f
    where 
          f.r_games > 20 
      and f.r_minutes_played_per_game > 15
)

, playererastats as (
    select
        player_name,
        season_start_year,
        case
            when season_start_year between 1997 and 2001 then '1997-2001 (late 90s)'
            when season_start_year between 2002 and 2006 then '2002-2006 (early 00s)'
            when season_start_year between 2007 and 2011 then '2007-2011 (mid 00s)'
            when season_start_year between 2012 and 2016 then '2012-2016 (early 10s)'
            when season_start_year between 2017 and 2021 then '2017-2021 (modern era)'
            else 'other'
        end as era_window,
        r_points_per_game
    from playerseasons
    where 
          era_window != 'other'
)

select
    era_window,
    player_name,
    avg(r_points_per_game) as avg_ppg_in_era,
    rank() over (partition by era_window order by avg(r_points_per_game) desc) as rank_in_era
from playererastats
group by 
    era_window,
    player_name
order by era_window, rank_in_era
