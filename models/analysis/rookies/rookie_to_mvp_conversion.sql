with player_stats as (
    select *
    from {{ ref('fct__regular_rookie_playoffs_mvps_full_players_stats_seasons_97_22') }}
)  

,rookies as (
  select player_id, 
         ro_player_name as player_name, 
         ro_seasons as rookie_season,
         ro_points_per_game as rookie_ppg, 
         ro_roty_points_won
  from player_stats
  where ro_seasons is not null
),

future_mvp as (
  select player_id, 
  player_name, 
  rsm_seasons as mvp_season
  from player_stats
  where rsm_seasons is not null
)

select distinct r.player_id, 
       r.player_name, 
       r.rookie_season, 
       r.rookie_ppg, 
       case 
            when f.player_id is not null then 1 
            else 0 
       end as later_became_mvp
from rookies r
left join future_mvp f on f.player_id = r.player_id
order by later_became_mvp desc, 
         r.rookie_ppg desc
