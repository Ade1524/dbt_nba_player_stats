-- 1️⃣8️⃣ Who are the greatest rookies in NBA history by statistical profile?
with player_stats as (
    select *
    from {{ ref('fct__regular_rookie_playoffs_mvps_full_players_stats_seasons_97_22') }}
) 

select
    ro.ro_player_name as player_name,
    ro.ro_seasons as season,
    ro.ro_teams_name as team_name,
    ro.ro_points_per_game as ppg,
    ro.ro_rebounds_per_game as rpg,
    ro.ro_assists_per_game as apg,
    ro.ro_pts_won_vs_pts_max as roty_share,
    
    -- create a simple composite score (weighted sum)
    (ro.ro_points_per_game * 0.4 + ro.ro_rebounds_per_game * 0.2 + ro.ro_assists_per_game * 0.2 + ro.ro_pts_won_vs_pts_max * 0.2) as composite_rookie_score,
    
    rank() over (order by composite_rookie_score desc) as rookie_rank
    
from player_stats ro
where ro.ro_player_name is not null -- player has rookie data
qualify row_number() over (partition by ro.ro_player_name order by ro.ro_seasons) = 1 -- only the rookie season
order by rookie_rank