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

,seasons_champion_teams as (
    select sn.seasons
          ,sn.champion_team as champions
          ,lt.teams
     from seasons_nba_champions sn
     left join list_teams lt
     on sn.champion_team = lt.team_name
     order by  sn.seasons
    
)

select * from seasons_champion_teams