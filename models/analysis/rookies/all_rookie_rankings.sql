with player_stats as (
    select *
    from {{ ref('fct__regular_rookie_playoffs_mvps_full_players_stats_seasons_97_22') }}
) 

,ro as (
  select ro_player_name as player_name, 
         ro_seasons as seasons,
         ro_points_per_game as ppg, 
         ro_rebounds_per_game as rpg, 
         ro_assists_per_game as apg,
         ro_roty_points_won as roty_points_won, 
         ro_win_loss_percentage
  from player_stats
  where ro_seasons is not null
),

score as (
  select *,
    (coalesce(ppg,0)*1.5 + coalesce(apg,0)*1.2 + coalesce(rpg,0)*1.1 + coalesce(roty_points_won,0)*2.0) as rookie_score
  from ro
)

select 
    distinct player_name, 
    seasons, 
    ppg, 
    rpg, 
    apg, 
    rookie_score
from score
order by rookie_score desc

