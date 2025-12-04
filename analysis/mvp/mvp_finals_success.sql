-- 1️⃣4️⃣ How often does the season MVP reach the finals — and win the championship?

with player_stats as (
    select *
    from {{ ref('fct__regular_rookie_playoffs_mvps_full_players_stats_seasons_97_22') }}
)

, dim_seasons as (
    select *
    from {{ ref('dim_nba_seasons') }}
)

, dim_teams as (
    select *
    from {{ ref('dim_nba_teams') }}
)

, mvpseasonsinfo as (
    select
        fm.dim_team_key,
        fm.dim_season_key,
        fm.seasons,
        fm.rsm_player_name,
        fm.r_team_name,
        fm.rsm_teams -- the team abbreviation of the mvp
    from player_stats fm
    where 
        fm.rsm_player_name is not null 
    qualify row_number() over (partition by fm.seasons order by fm.rsm_voting desc) = 1 -- select the actual mvp (highest voting share)
)


, mvpfinalsresult as (
    select
        mvp.dim_team_key,
        mvp.dim_season_key, 
        mvp.rsm_player_name as player_name,
        mvp.seasons,
        mvp.r_team_name as team_name,
        mvp.rsm_teams as teams,
        s.champions, -- champion team name from dim_nba_seasons
        s.runner_up, -- assuming dim_nba_seasons has a runner_up column     
        case
            when mvp.r_team_name = s.champions then 'champion'
            when mvp.r_team_name = s.runner_up then 'finalist'
            else 'no finals appearance'
        end as finals_result
    from mvpseasonsinfo mvp
    left join dim_seasons s on mvp.dim_season_key = s.dim_season_key
    left join dim_teams t on mvp.dim_team_key = t.dim_team_key -- join to find the winner's abbreviation for comparison
    order by 
        mvp.seasons
)

, final as (
    select 
         distinct seasons as season
        ,champions
        ,runner_up
        ,player_name as mvp_player_name
        ,team_name as mvp_team_name
        ,finals_result
    from mvpfinalsresult
    order by  seasons
)


select
    *
from final
order by  season