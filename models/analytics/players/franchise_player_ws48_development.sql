

with player_stats as (
    select *
    from {{ ref('fct__regular_rookie_playoffs_mvps_full_players_stats_seasons_97_22') }}
)

,dim_teams as (
    select *
    from {{ ref('dim_nba_teams') }}
)

,playeryearoveryear as (
    select
        f.player_name,
        f.dim_team_key,
        f.r_teams,
        f.seasons,
        {{ extract_season_start_year('seasons') }} as season_start_year,
        {{ extract_season_end_year('seasons') }} as season_end_year,
        f.p_win_share_per_48_games as current_ws48,
        lag(f.p_win_share_per_48_games, 1) over (partition by f.player_name order by season_start_year) as previous_ws48,
        lag(f.r_teams, 1) over (partition by f.player_name order by season_start_year) as previous_team
    from player_stats f
    where 
        f.r_games >= 41 
    and f.p_win_share_per_48_games is not null
),

teamdevelopment as (
    select
        p.r_teams as current_team_abbr,
        t.team_name,
        p.current_ws48 - p.previous_ws48 as ws48_improvement
    from playeryearoveryear p
    join dim_teams t
        on p.dim_team_key = t.dim_team_key
    where 
        p.current_ws48 > p.previous_ws48
    and p.r_teams = p.previous_team
)

select
    team_name,
    avg(ws48_improvement) as avg_player_development_metric,
    count(*) as total_improvement_instances
from teamdevelopment
group by 1
having 
    count(*) > 10
order by 
    avg_player_development_metric desc
