-- 2️⃣0️⃣ Which teams consistently produce high-impact rookies?

with player_stats as (
    select *
    from {{ ref('fct__regular_rookie_playoffs_mvps_full_players_stats_seasons_97_22') }}
) 

, rookiesfiltered as (
    -- step 1: filter the fact table to isolate the true rookie season for each player.
    select
        ro.ro_teams_name,
        ro.ro_pts_won_vs_pts_max,
        ro.ro_player_name,
        -- assign a rank (rn) to find the player's first recorded season (rn=1)
        row_number() over (partition by ro.ro_player_name order by ro.ro_seasons) as rn
    from  player_stats ro
    where ro.ro_player_name is not null
)

select
    rf.ro_teams_name as team_name,
    -- step 2: aggregate the average roty voting share for these single rookie seasons.
    avg(rf.ro_pts_won_vs_pts_max) as avg_roty_vote_share,
    count(distinct rf.ro_player_name) as total_rookies_analyzed 
from rookiesfiltered rf
where 
    rf.rn = 1 -- select only the first season (the rookie season)
group by
    rf.ro_teams_name
having 
    count(distinct rf.ro_player_name) >= 5 -- require a minimum number of rookies analyzed per team
order by 
    avg_roty_vote_share desc
