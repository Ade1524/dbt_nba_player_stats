-- 1️⃣5️⃣ How has MVP player type changed over time (guards vs wings vs bigs)?

with player_stats as (
    select *
    from {{ ref('fct__regular_rookie_playoffs_mvps_full_players_stats_seasons_97_22') }}
)

,mvptypebyyear as (
    select
        rsm.rsm_player_name as player_name,
        rsm.seasons as seasons,
        lower (r.r_position) as position,
        {{ extract_season_start_year('rsm.seasons') }} as season_start_year,
        {{ extract_season_end_year('rsm.seasons') }} as season_end_year,
        
        case
            when position in ('pg', 'sg') then 'guard'
            when position in ('sf', 'pf') then 'wing/forward'
            when position = 'c' then 'big'
            else 'other'
        end as player_archetype
        
    from player_stats rsm
    join player_stats r
        on rsm.dim_player_key = r.dim_player_key and rsm.dim_season_key = r.dim_season_key
    where 
        rsm.rsm_player_name is not null
    qualify row_number() over (partition by rsm.seasons order by rsm.rsm_voting desc) = 1
)

, decadearchetypecounts as (
    select
        player_name,
        seasons,
        position,
        floor(season_start_year / 10) * 10 as decade,
        player_archetype,
        count(*) as mvp_count
    from mvptypebyyear
    group by 
        player_name,
        seasons,
        position,
        season_start_year,
        player_archetype
)


select
    decade,
    player_name,
    player_archetype,
    mvp_count,
    sum(mvp_count) over (partition by decade) as total_mvps_in_decade,
    (mvp_count * 1.0 / sum(mvp_count) over (partition by decade)) as mvp_share_in_decade
from decadearchetypecounts
order by 
    decade,
    mvp_count desc