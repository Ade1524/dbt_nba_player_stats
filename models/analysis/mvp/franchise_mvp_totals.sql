-- 1️⃣6️⃣ Which franchises have produced the most MVPs and Finals MVPs?

with player_stats as (
    select *
    from {{ ref('fct__regular_rookie_playoffs_mvps_full_players_stats_seasons_97_22') }}
) 

, dim_teams as (
    select *
    from {{ ref('dim_nba_teams') }}
)

, mvpfranchise as (
    select
        rsm.rsm_player_name,
        rsm.rsm_teams,
        rsm.seasons
    from player_stats rsm
    where 
        rsm.rsm_player_name is not null
    qualify row_number() over (partition by rsm.seasons order by rsm.rsm_voting desc) = 1
)

, finalsmvpfranchise as (
    select
        fm.fm_player_name,
        fm.fm_teams,
        fm.seasons
    from player_stats fm
    where 
        fm.fm_player_name is not null
)

select
    t.team_name as franchise_name,
    count(distinct mvp.seasons) as total_season_mvps,
    count(distinct fmvp.seasons) as total_finals_mvps
from dim_teams t
left join mvpfranchise mvp on t.teams = mvp.rsm_teams
left join finalsmvpfranchise fmvp on t.teams = fmvp.fm_teams
group by franchise_name
order by 
    total_season_mvps desc, 
    total_finals_mvps desc
