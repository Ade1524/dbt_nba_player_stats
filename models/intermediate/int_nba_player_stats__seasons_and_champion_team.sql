with seasons_nba_champions as (
    select *
    from {{ ref('stg_nba_player_stats__nba_champions_team')}}
    where league like 'NBA'
    order by seasons
)

,list_teams as (
    select *
    from {{ ref('stg_nba_player_stats__nba_teams')}}
)


,final_season as (
    Select
        sn.seasons,
        sc.teams,
        sc.team_name as champions,
        sr.team_name as runner_up
    from seasons_nba_champions sn
    join list_teams sc on sn.champion_team = sc.team_name
    join list_teams sr on sn.runner_up__team = sr.team_name
    order by  sn.seasons

)

select 
    distinct seasons,
    teams,
    champions,
    runner_up
from final_season
order by seasons