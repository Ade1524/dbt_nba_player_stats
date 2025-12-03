with team_champion as (
    select * 
    from {{ ref('nba_champions')}}
)

, renaming_champions_columns as (
    select {{ get_season('year') }} as seasons
            ,initcap(lower(champion)) as champion_team
            ,initcap(lower(runner_up)) as runner_up__team
            ,league
    from team_champion
    order by seasons
)

select * 
from renaming_champions_columns