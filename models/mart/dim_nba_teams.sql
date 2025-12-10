with nba_teams_table as (
    select *
    from {{ ref('stg_nba_player_stats__nba_teams') }}
)
 
, teams_unique_id as (
    select {{ dbt_utils.generate_surrogate_key(['teams', 'team_name']) }} as dim_team_key
          ,teams
          ,team_name
    from nba_teams_table
)



select * 
from teams_unique_id

