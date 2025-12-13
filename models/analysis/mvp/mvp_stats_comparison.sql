with player_stats as (
    select *
    from {{ ref('fct__regular_rookie_playoffs_mvps_full_players_stats_seasons_97_22') }}
)

,candidates as (
  select player_id, 
         player_name, 
         seasons, 
         rsm_points_per_game as points,
         rsm_assists_per_game as assists, 
         rsm_win_share as ws, 
         rsm_voting as voting
  from player_stats
  where rsm_seasons is not null
)

select
  player_id, 
  player_name, 
  seasons,
  points, 
  assists, 
  ws, 
  voting
from candidates
order by voting desc,
         seasons
