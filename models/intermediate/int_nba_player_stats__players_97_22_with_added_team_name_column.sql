with player_stats_97_2022 as (
     select *
     from {{ ref('stg_nba_player_stats__players_season')}}
)

, player_stats_no_tot as (
    select  *
    from player_stats_97_2022
    where teams not like 'TOT'
)
, nba_teams as (
    select * 
    from {{ ref('stg_nba_player_stats__nba_teams') }}
)
, add_teams_name as (
    select 
        nt.team_name as team_name,
        pp.*  
    from player_stats_no_tot pp 
    left join nba_teams nt on pp.teams = nt.teams
     
)

select * from add_teams_name
  



