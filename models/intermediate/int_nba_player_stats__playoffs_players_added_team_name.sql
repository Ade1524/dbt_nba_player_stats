with player_playoffs_stats as (
    select *
      from {{ ref('stg_nba_player_stats__players_playoff') }}
)
, nba_teams as (
    select * 
      from {{ ref('stg_nba_player_stats__nba_teams') }}
)
, add_teams_name as (
    select nt.team_name
          ,pp.*
      from player_playoffs_stats pp 
      left join nba_teams nt 
      on nt.teams = pp.teams
     order by pp.seasons
)

select * from add_teams_name

