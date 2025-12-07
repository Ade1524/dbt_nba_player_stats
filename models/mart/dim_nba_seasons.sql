with seasons_table as (
    select *
    from {{ ref('int_nba_player_stats__seasons_and_champion_team') }}
)
, season_id as (
    select {{ dbt_utils.generate_surrogate_key(['seasons']) }} as dim_season_key
          ,seasons
          ,teams
          ,champions
          ,runner_up
    from seasons_table
)

, final as (
    select dim_season_key as dim_season_key
          ,seasons
          ,teams
          ,champions
          ,runner_up
    from season_id
)

select * 
from final
