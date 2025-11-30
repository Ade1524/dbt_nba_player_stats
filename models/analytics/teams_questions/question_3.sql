with regular_season_stats as (
    -- Aggregate player stats by team for regular season
    select
        f.seasons,
        f.dim_season_key,
        t.dim_team_key,
        t.team_name,
        t.teams,
        s.seasons as regular_season,
        
        -- Games played
        sum(f.r_games) as total_games,
        avg(f.r_games) as avg_games_per_player,
        
        -- Points
        sum(f.r_points_per_game) as team_total_points,
        avg(f.r_points_per_game) as avg_points_per_game,
        sum(f.r_points_per_game) / nullif(sum(f.r_games), 0) as team_points_per_game,
        
        -- Rebounds
        sum(f.r_total_rebounds_per_game) as team_total_rebounds_per_game,
        avg(f.r_total_rebounds_per_game) as avg_rebounds_per_game,
        sum(f.r_total_rebounds_per_game) / nullif(sum(f.r_games), 0) as team_rebounds_per_game,
        
        -- Assists
        sum(f.r_assists_per_game) as team_total_assists,
        avg(f.r_assists_per_game) as avg_assists_per_game,
        sum(f.r_assists_per_game) / nullif(sum(f.r_games), 0) as team_assists_per_game,
        
        -- Steals
        sum(f.r_steals_per_game) as team_total_steals,
        sum(f.r_steals_per_game) / nullif(sum(f.r_games), 0) as team_steals_per_game,
        
        -- Blocks
        sum(f.r_blocks_per_game) as team_total_blocks,
        sum(f.r_blocks_per_game) / nullif(sum(f.r_games), 0) as team_blocks_per_game,
        
        -- Turnovers
        sum(f.r_turnovers_per_game) as team_total_turnovers,
        sum(f.r_turnovers_per_game) / nullif(sum(f.r_games), 0) as team_turnovers_per_game,
        
        -- Shooting percentages (weighted by attempts)
        sum(f.r_field_goals_per_game) / nullif(sum(f.r_field_goal_attempts_per_game), 0) as team_fg_percentage,
        sum(f.r_c_3_point_field_goals_per_game) / nullif(sum(f.r_c_3_point_field_goal_attempts_per_game), 0) as team_3p_percentage,
        sum(f.r_free_throws_per_game) / nullif(sum(f.r_free_throw_attempts_per_game), 0) as team_ft_percentage,
        
        -- Advanced metrics
        sum(f.r_field_goal_attempts_per_game) / nullif(sum(f.r_games), 0) as team_fga_per_game,
        sum(f.r_c_3_point_field_goal_attempts_per_game) / nullif(sum(f.r_games), 0) as team_3pa_per_game,
        
        -- Player count
        count(distinct f.player_id) as roster_size,
        
        -- Performance indicators
        avg(f.p_player_efficiency_rating) as avg_team_per,
        sum(f.p_win_share) as team_win_shares
        
    from {{ ref('fct__regular_rookie_playoffs_mvps_full_players_stats_seasons_97_22') }} f
    join {{ ref('dim_nba_teams') }} t on f.dim_team_key = t.dim_team_key
    join {{ ref('dim_nba_seasons') }} s on f.dim_season_key = s.dim_season_key
    group by 
        f.seasons,
        f.dim_season_key,
        t.dim_team_key,
        t.team_name,
        t.teams,
        s.seasons
    order by s.seasons asc
)

,playoff_stats as (
    -- Aggregate player stats by team for playoffs
    select
        f.seasons,
        f.dim_season_key,
        t.dim_team_key,
        t.team_name,
        t.teams,
        s.seasons as playoff_season,
        
        
        -- Games played (playoff games)
        sum(f.r_games - f.p_playoff_games_in_season) as playoff_games,
        avg(f.r_games - f.p_playoff_games_in_season) as avg_playoff_games_per_player,
        
        -- Points
        sum(f.p_points) as playoff_total_points,
        avg(f.p_points) as avg_playoff_points_per_game,
        sum(f.p_points) / nullif(sum(f.p_playoff_games_in_season), 0) as playoff_team_points_per_game,
        
        -- Rebounds
        sum(f.p_total_rebounds_per_game) as playoff_p_total_rebounds_per_game,
        avg(f.p_total_rebounds_per_game) as avg_playoff_rebounds_per_game,
        sum(f.p_total_rebounds_per_game) / nullif(sum(f.p_playoff_games_in_season), 0) as playoff_team_rebounds_per_game,
        
        -- Assists
        sum(f.p_assists_per_game) as playoff_total_assists,
        avg(f.p_assists_per_game) as avg_playoff_assists_per_game,
        sum(f.p_assists_per_game) / nullif(sum(f.p_playoff_games_in_season), 0) as playoff_team_assists_per_game,
        
        -- Steals
        sum(f.p_steals_per_game) as playoff_total_steals,
        sum(f.p_steals_per_game) / nullif(sum(f.p_playoff_games_in_season), 0) as playoff_team_steals_per_game,
        
        -- Blocks
        sum(f.p_blocks_per_game) as playoff_total_blocks,
        sum(f.p_blocks_per_game) / nullif(sum(f.p_playoff_games_in_season), 0) as playoff_team_blocks_per_game,
        
        -- Turnovers
        sum(f.p_turnovers_per_game) as playoff_total_turnovers,
        sum(f.p_turnovers_per_game) / nullif(sum(f.p_playoff_games_in_season), 0) as playoff_team_turnovers_per_game,
        
        -- Shooting percentages
        sum(f.p_field_goals_per_game) / nullif(sum(f.p_field_goal_attempts_per_game), 0) as playoff_team_fg_percentage,
        sum(f.p_a_3_point_field_goals_per_game) / nullif(sum(f.p_a_3_point_field_goal_attempts_per_game), 0) as playoff_team_3p_percentage,
        sum(f.p_free_throws_per_game) / nullif(sum(f.p_free_throw_attempts_per_game), 0) as playoff_team_ft_percentage,
        
        -- Advanced metrics
        sum(f.p_field_goal_attempts_per_game) / nullif(sum(f.p_playoff_games_in_season), 0) as playoff_team_fga_per_game,
        sum(f.p_a_3_point_field_goal_attempts_per_game) / nullif(sum(f.p_playoff_games_in_season), 0) as playoff_team_3pa_per_game,
        
        -- Player count
        count(distinct f.player_id) as playoff_roster_size,
        
        -- Performance indicators
        avg(f.p_player_efficiency_rating) as avg_playoff_team_per,
        sum(f.p_win_share) as playoff_team_win_shares,
        
        -- Playoff depth (how many games they played indicates playoff success)
        max(f.r_games - f.p_playoff_games_in_season ) as max_playoff_games_by_player
        
    from {{ ref('fct__regular_rookie_playoffs_mvps_full_players_stats_seasons_97_22') }} f
    join {{ ref('dim_nba_teams') }} t on f.dim_team_key = t.dim_team_key
    join {{ ref('dim_nba_seasons') }} s on f.dim_season_key = s.dim_season_key
    group by 
        f.seasons,
        f.dim_season_key,
        t.dim_team_key,
        t.team_name,
        t.teams,
        s.seasons
        
)

,

combined_performance as (
    -- Combine regular season and playoff stats
    select
        rs.dim_season_key,
        rs.dim_team_key,
        rs.team_name,
        rs.teams,
        rs.regular_season,
        ps.playoff_season,
        
        -- Regular Season Stats
        rs.total_games as regular_season_games,
        rs.team_points_per_game as regular_ppg,
        rs.team_rebounds_per_game as regular_rpg,
        rs.team_assists_per_game as regular_apg,
        rs.team_steals_per_game as regular_spg,
        rs.team_blocks_per_game as regular_bpg,
        rs.team_turnovers_per_game as regular_tpg,
        rs.team_fg_percentage as regular_fg_pct,
        rs.team_3p_percentage as regular_3p_pct,
        rs.team_ft_percentage as regular_ft_pct,
        rs.avg_team_per as regular_per,
        rs.team_win_shares as regular_win_shares,
        
        -- Playoff Stats
        ps.playoff_games,
        ps.playoff_team_points_per_game as playoff_ppg,
        ps.playoff_team_rebounds_per_game as playoff_rpg,
        ps.playoff_team_assists_per_game as playoff_apg,
        ps.playoff_team_steals_per_game as playoff_spg,
        ps.playoff_team_blocks_per_game as playoff_bpg,
        ps.playoff_team_turnovers_per_game as playoff_tpg,
        ps.playoff_team_fg_percentage as playoff_fg_pct,
        ps.playoff_team_3p_percentage as playoff_3p_pct,
        ps.playoff_team_ft_percentage as playoff_ft_pct,
        ps.avg_playoff_team_per as playoff_per,
        ps.playoff_team_win_shares as playoff_win_shares,
        ps.max_playoff_games_by_player,
        
        -- Calculate differences (Playoff vs Regular Season)
        ps.playoff_team_points_per_game - rs.team_points_per_game as ppg_improvement,
        ps.playoff_team_rebounds_per_game - rs.team_rebounds_per_game as rpg_improvement,
        ps.playoff_team_assists_per_game - rs.team_assists_per_game as apg_improvement,
        ps.playoff_team_steals_per_game - rs.team_steals_per_game as spg_improvement,
        ps.playoff_team_blocks_per_game - rs.team_blocks_per_game as bpg_improvement,
        rs.team_turnovers_per_game - ps.playoff_team_turnovers_per_game as turnover_reduction,
        
        -- Shooting improvements
        (ps.playoff_team_fg_percentage - rs.team_fg_percentage) * 100 as fg_pct_improvement,
        (ps.playoff_team_3p_percentage - rs.team_3p_percentage) * 100 as three_pt_pct_improvement,
        (ps.playoff_team_ft_percentage - rs.team_ft_percentage) * 100 as ft_pct_improvement,
        
        -- Performance metrics improvements
        ps.avg_playoff_team_per - rs.avg_team_per as per_improvement,
        
        -- Playoff success indicators
        case 
            when ps.playoff_games >= 16 then 'Made Finals'
            when ps.playoff_games >= 12 then 'Made Conference Finals'
            when ps.playoff_games >= 8 then 'Made Conference Semi-Finals'
            when ps.playoff_games >= 4 then 'Made Playoffs'
            else 'Early Exit'
        end as playoff_round_reached,
        
        -- Calculate overall performance improvement score
        (
            ((ps.playoff_team_points_per_game - rs.team_points_per_game) / nullif(rs.team_points_per_game, 0) * 30) +
            ((ps.playoff_team_rebounds_per_game - rs.team_rebounds_per_game) / nullif(rs.team_rebounds_per_game, 0) * 15) +
            ((ps.playoff_team_assists_per_game - rs.team_assists_per_game) / nullif(rs.team_assists_per_game, 0) * 15) +
            ((ps.playoff_team_fg_percentage - rs.team_fg_percentage) / nullif(rs.team_fg_percentage, 0) * 20) +
            ((ps.avg_playoff_team_per - rs.avg_team_per) / nullif(rs.avg_team_per, 0) * 20)
        ) as overall_playoff_improvement_score
        
    from regular_season_stats rs
    inner join playoff_stats ps
        on rs.dim_season_key = ps.dim_season_key
        and rs.dim_team_key = ps.dim_team_key
)

select
    regular_season as season_year,
    team_name,
    teams,
    playoff_round_reached,
    playoff_games,
    
    -- Regular Season Performance
    round(regular_ppg, 2) as regular_season_ppg,
    round(regular_rpg, 2) as regular_season_rpg,
    round(regular_apg, 2) as regular_season_apg,
    round(regular_fg_pct * 100, 2) as regular_season_fg_pct,
    round(regular_3p_pct * 100, 2) as regular_season_3p_pct,
    round(regular_per, 2) as regular_season_per,
    
    -- Playoff Performance
    round(playoff_ppg, 2) as playoff_ppg,
    round(playoff_rpg, 2) as playoff_rpg,
    round(playoff_apg, 2) as playoff_apg,
    round(playoff_fg_pct * 100, 2) as playoff_fg_pct,
    round(playoff_3p_pct * 100, 2) as playoff_3p_pct,
    round(playoff_per, 2) as playoff_per,
    
    -- Improvements
    round(ppg_improvement, 2) as points_improvement,
    round(rpg_improvement, 2) as rebounds_improvement,
    round(apg_improvement, 2) as assists_improvement,
    round(fg_pct_improvement, 2) as fg_pct_improvement_points,
    round(three_pt_pct_improvement, 2) as three_pt_pct_improvement_points,
    round(per_improvement, 2) as per_improvement,
    round(turnover_reduction, 2) as turnover_reduction,
    
    -- Overall Score
    round(overall_playoff_improvement_score, 2) as playoff_performance_score,
    
    -- Classification
    case 
        when overall_playoff_improvement_score >= 10 then 'Significantly Exceeded Expectations'
        when overall_playoff_improvement_score >= 5 then 'Exceeded Expectations'
        when overall_playoff_improvement_score >= 0 then 'Met Expectations'
        when overall_playoff_improvement_score >= -5 then 'Below Expectations'
        else 'Significantly Below Expectations'
    end as performance_classification

from combined_performance
where playoff_games >= 4  -- Only teams that made playoffs
order by overall_playoff_improvement_score desc



