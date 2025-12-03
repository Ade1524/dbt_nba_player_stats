with nba_teams as (
    select * 
    from {{ ref('nba_teams')}}
)
,rename_knicks_team as (
    select 
           team_name_abbreviation as teams
          ,lower(replace(replace(team_name,'Knickerbockers','Knicks'),'Trailblazers','Trail blazers')) as team_name
    from nba_teams 
)

,rename_utah_team as (
    select 
           (replace(teams,'UTH','UTA')) as teams
          ,team_name
    from rename_knicks_team 
)

select 
       teams
      ,initcap(team_name) as team_name
from rename_utah_team



