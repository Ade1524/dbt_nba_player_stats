-- 1️⃣3️⃣ what stats most strongly predict winning the season mvp?

with player_stats as (
    select *
    from {{ ref('fct__regular_rookie_playoffs_mvps_full_players_stats_seasons_97_22') }}
)

, mvpstats as (
    select
        rsm.seasons,
        rsm.player_name,
        rsm.rsm_points_per_game,
        rsm.rsm_assists_per_game,
        rsm.rsm_total_rebounds_per_game,
        rsm.rsm_win_share_per_48_games
    from player_stats rsm
    where 
        rsm.rsm_player_name is not null -- player received mvp votes (or was the mvp)
    order by 
        seasons
)

,leagueavgstats as (
    select
        r.seasons,
        r.player_name,
        r.r_points_per_game as league_avg_ppg,
        r_assists_per_game as league_avg_apg,
        r_total_rebounds_per_game as league_avg_rpg
    from player_stats r
    -- filter to get a representative sample of regular season player performance
    where 
        r.r_games >= 41 
    and r.r_minutes_played_per_game >= 20
    order by 
        seasons
)

select
    m.seasons as mvp_season,
    l.seasons as regular_season,
    m.player_name,
    'mvp_year' as mvp_category,
    m.rsm_points_per_game as avg_ppg,
    m.rsm_assists_per_game as avg_apg,
    m.rsm_total_rebounds_per_game as avg_rpg,
    'regular_season_year' as regular_season_category,
    l.league_avg_ppg,
    l.league_avg_apg,
    l.league_avg_rpg
from mvpstats m
left join leagueavgstats l on m.player_name = l.player_name


