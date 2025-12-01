-- 1️⃣9️⃣ Do elite rookie seasons predict future MVP-level performance?

with player_stats as (
    select *
    from {{ ref('fct__regular_rookie_playoffs_mvps_full_players_stats_seasons_97_22') }}
)  

,eliterookies as (
    select
        ro.ro_player_name,
        ro.ro_seasons,
        (ro.ro_points_per_game * 0.4 + ro.ro_rebounds_per_game * 0.2 + ro.ro_assists_per_game * 0.2 + ro.ro_pts_won_vs_pts_max * 0.2) as composite_rookie_score
    from player_stats ro
    where 
        ro.ro_player_name is not null
    qualify row_number() over (partition by ro.ro_player_name order by ro.ro_seasons) = 1
    order by 
        composite_rookie_score desc
    limit 50 -- select the top 50 statistical rookies
),

careermvps as (
    select distinct
        rsm.rsm_player_name as mvp_winner
    from  
        player_stats rsm
    where 
        rsm.rsm_player_name is not null
)

select
    er.ro_player_name as player_name,
    er.ro_seasons as season,
    er.composite_rookie_score,
    case when cm.mvp_winner is not null then 'yes' else 'no' end as won_career_mvp
from eliterookies er
left join careermvps cm
    on er.ro_player_name = cm.mvp_winner