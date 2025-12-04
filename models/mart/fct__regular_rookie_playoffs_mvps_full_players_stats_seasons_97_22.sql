with playoffs as (
    select *
    from {{ ref('int_nba_player_stats__playoffs_players_added_team_name') }}
)

,regular as (
    select *    
    from {{ ref('int_nba_player_stats__players_97_22_with_added_team_name_column') }}
)

, first_regular_season as (
    select player_id,
           min(seasons) as seasons
    from regular
    group by all
)

,rookies as (
    select *    
    from {{ ref('stg_nba_player_stats__nba_total_rookies_stats_1980_2023') }}
)

,regular_season_mvp as (
    select *    
    from {{ ref('stg_nba_player_stats__regular_season_mvp_player_stats') }}
)

,finals_mvp as (
    select *    
    from {{ ref('stg_nba_player_stats__playoffs_mvp_finals') }}
)
,final_regular as (

    select  
           {{ dbt_utils.generate_surrogate_key(['r.seasons']) }} as dim_season_key
          ,{{ dbt_utils.generate_surrogate_key(['r.player_id', 'r.player_name', 'r.year_of_birth']) }} as dim_player_key
          ,{{ dbt_utils.generate_surrogate_key(['r.teams', 'r.team_name']) }} as dim_team_key
          

-- Seasons 97-22 player stats columns          
          ,r.player_id as player_id
          ,r.seasons as seasons
          ,r.player_name as player_name
          ,r.year_of_birth as year_of_birth
          ,r.position as r_position
          ,r.age as r_age
          ,r.teams as r_teams
          ,r.team_name as r_team_name
          ,r.games as r_games
          ,r.games_started as r_games_started
          ,r.minutes_played_per_game as r_minutes_played_per_game
          ,r.field_goals_per_game as r_field_goals_per_game
          ,r.field_goal_attempts_per_game as r_field_goal_attempts_per_game
          ,r.field_goal_percentage as r_field_goal_percentage
          ,r.c_3_point_field_goals_per_game as r_c_3_point_field_goals_per_game
          ,r.c_3_point_field_goal_attempts_per_game as r_c_3_point_field_goal_attempts_per_game
          ,r.c_3_point_field_goal_percentage as r_c_3_point_field_goal_percentage
          ,r.c_2_point_field_goals_per_game as r_c_2_point_field_goals_per_game
          ,r.c_2_point_field_goal_attempts_per_game as r_c_2_point_field_goal_attempts_per_game
          ,r.c_2_point_field_goal_percentage as r_c_2_point_field_goal_percentage
          ,r.effective_field_goal_percentage as r_effective_field_goal_percentage
          ,r.free_throws_per_game as r_free_throws_per_game
          ,r.free_throw_attempts_per_game as r_free_throw_attempts_per_game
          ,r.free_throw_percentage as r_free_throw_percentage
          ,r.offensive_rebounds_per_game as r_offensive_rebounds_per_game
          ,r.defensive_rebounds_per_game as r_defensive_rebounds_per_game
          ,r.total_rebounds_per_game as r_total_rebounds_per_game
          ,r.assists_per_game as r_assists_per_game
          ,r.steals_per_game as r_steals_per_game
          ,r.blocks_per_game as r_blocks_per_game
          ,r.turnovers_per_game as r_turnovers_per_game
          ,r.personal_fouls_per_game as r_personal_fouls_per_game
          ,r.points_per_game as r_points_per_game
          ,(substring(frs.seasons, 1, 4)::number - substring(ro.seasons, 1, 4)::number) + row_number() over (partition by r.player_id order by r.seasons) as num_years_in_league

-- Playoffs player stats columns
          ,p.player_name as p_player_name
          ,p.seasons as p_seasons           
          ,p.team_name as p_team_name
          ,p.position as p_position
          ,p.age as p_age
          ,p.teams as p_teams
          ,p.playoff_games_in_season as p_playoff_games_in_season
          ,p.playoff_games_started_in_season as p_playoff_games_started_in_season
          ,p.minutes_played_per_game as p_minutes_played_per_game
          ,p.field_goals_per_game as p_field_goals_per_game
          ,p.field_goal_attempts_per_game as p_field_goal_attempts_per_game
          ,p.field_goal_percentage as p_field_goal_percentage
          ,p.a_3_point_field_goals_per_game as p_a_3_point_field_goals_per_game
          ,p.a_3_point_field_goal_attempts_per_game as p_a_3_point_field_goal_attempts_per_game
          ,p.a_3_point_field_goal_percentage as p_a_3_point_field_goal_percentage
          ,p.a_2_point_field_goals_per_game as p_a_2_point_field_goals_per_game
          ,p.a_2_point_field_goal_attempts_per_game as p_a_2_point_field_goal_attempts_per_game
          ,p.a_2_point_field_goal_percentage as p_a_2_point_field_goal_percentage
          ,p.effective_field_goal_percentage as p_effective_field_goal_percentage
          ,p.free_throws_per_game as p_free_throws_per_game
          ,p.free_throw_attempts_per_game as p_free_throw_attempts_per_game
          ,p.free_throw_percentage as p_free_throw_percentage
          ,p.offensive_rebounds_per_game as p_offensive_rebounds_per_game
          ,p.defensive_rebounds_per_game as p_defensive_rebounds_per_game
          ,p.total_rebounds_per_game as p_total_rebounds_per_game
          ,p.assists_per_game as p_assists_per_game
          ,p.steals_per_game as p_steals_per_game
          ,p.blocks_per_game as p_blocks_per_game
          ,p.turnovers_per_game as p_turnovers_per_game
          ,p.personal_fouls_per_game as p_personal_fouls_per_game
          ,p.points as p_points
          ,p.assist_percentage as p_assist_percentage
          ,p.block_percentage as p_block_percentage
          ,p.box_plus_minus as p_box_plus_minus
          ,p.defensive_box_plus_minus as p_defensive_box_plus_minus
          ,p.defensive_rebounding_percentage as p_defensive_rebounding_percentage
          ,p.defensive_win_share as p_defensive_win_share
          ,p.a_3_point_shot_attempts_per_field_goal_attempted as p_a_3_point_shot_attempts_per_field_goal_attempted
          ,p.free_throw_attempted_per_field_goal_attempted_percentage as p_free_throw_attempted_per_field_goal_attempted_percentage
          ,p.total_minutes_played as p_total_minutes_played
          ,p.offensive_box_plus_minus as p_offensive_box_plus_minus
          ,p.offensive_rebounding_percentage as p_offensive_rebounding_percentage
          ,p.offensive_win_share as p_offensive_win_share
          ,p.player_efficiency_rating as p_player_efficiency_rating
          ,p.steal_percentage as p_steal_percentage
          ,p.turnover_percentage as p_turnover_percentage
          ,p.total_rebound_percentage as p_total_rebound_percentage
          ,p.true_shooting_percentage as p_true_shooting_percentage
          ,p.usage_percentage as p_usage_percentage
          ,p.value_over_replacement_player as p_value_over_replacement_player
          ,p.win_share as p_win_share
          ,p.win_share_per_48_games as p_win_share_per_48_games
 
--  rookies stats columns
          ,ro.seasons as ro_seasons 
          ,ro.player_name as ro_player_name 
          ,ro.teams as ro_teams 
          ,ro.teams_name as ro_teams_name 
          ,ro.age as ro_age 
          ,ro.years_in_the_league as ro_years_in_the_league 
          ,ro.games_played as ro_games_played 
          ,ro.minutes_played as ro_minutes_played 
          ,ro.field_goals as ro_field_goals  
          ,ro.field_goals_attempted as ro_field_goals_attempted 
          ,ro.c_3_pointers as ro_c_3_pointers  
          ,ro.c_3_points_attempted as ro_c_3_points_attempted 
          ,ro.free_throws as ro_free_throws 
          ,ro.free_throws_attempted as ro_free_throws_attempted 
          ,ro.offensive_rebound as ro_offensive_rebound 
          ,ro.true_rebound as ro_true_rebound 
          ,ro.assists as ro_assists 
          ,ro.steals as ro_steals 
          ,ro.blocks as ro_blocks 
          ,ro.turnovers as ro_turnovers 
          ,ro.personal_fouls as ro_personal_fouls 
          ,ro.points as ro_points 
          ,ro.c_field_goal_percentage as ro_c_field_goal_percentage 
          ,ro.c_3p_point_percentage as ro_c_3p_point_percentage 
          ,ro.free_throw_percentage as ro_free_throw_percentage 
          ,ro.minutes_per_game as ro_minutes_per_game 
          ,ro.points_per_game as ro_points_per_game 
          ,ro.rebounds_per_game as ro_rebounds_per_game 
          ,ro.assists_per_game as ro_assists_per_game
          ,ro.roty_points_won as ro_roty_points_won
          ,ro.max_amount_of_points_you_can_win as ro_max_amount_of_points_you_can_win
          ,ro.pts_won_vs_pts_max as ro_pts_won_vs_pts_max
          ,ro.win_total_during_the_season as ro_win_total_during_the_season
          ,ro.lose_total_during_the_season as ro_lose_total_during_the_season
          ,ro.win_loss_percentage as ro_win_loss_percentage
          ,ro.team_games_behind as ro_team_games_behind
          ,ro.points_per_game_in_the_team as ro_points_per_game_in_the_team
          ,ro.opponents_points_per_game as ro_opponents_points_per_game
          ,ro.simple_rating_system as ro_simple_rating_system

-- Regular season mvp stats columns         
          ,rsm.seasons as rsm_seasons
          ,rsm.league as rsm_league
          ,rsm.player_name as rsm_player_name
          ,rsm.age as rsm_age
          ,rsm.teams as rsm_teams
          ,rsm.games as rsm_games
          ,rsm.minutes_played_per_games as rsm_minutes_played_per_games
          ,rsm.points_per_game as rsm_points_per_game
          ,rsm.total_rebounds_per_game as rsm_total_rebounds_per_game
          ,rsm.assists_per_game as rsm_assists_per_game
          ,rsm.steals_per_game as rsm_steals_per_game
          ,rsm.blocks_per_game as rsm_blocks_per_game
          ,rsm.field_goal_percentage as rsm_field_goal_percentage
          ,rsm.c_3_point_field_goal_percentage as rsm_c_3_point_field_goal_percentage
          ,rsm.free_throw_percentage as rsm_free_throw_percentage
          ,rsm.win_share as rsm_win_share
          ,rsm.win_share_per_48_games as rsm_win_share_per_48_games
          ,rsm.voting as rsm_voting
          
-- Finals mvp stats columns
          ,fm.seasons as fm_seasons
          ,fm.league as fm_league
          ,fm.player_name as fm_player_name
          ,fm.age as fm_age
          ,fm.teams as fm_teams
          ,fm.games as fm_games
          ,fm.minutes_played_per_game as fm_minutes_played_per_game
          ,fm.points_per_game as fm_points_per_game
          ,fm.total_rebounds_per_game as fm_total_rebounds_per_game
          ,fm.assists_per_game as fm_assists_per_game
          ,fm.steals_per_game as fm_steals_per_game
          ,fm.blocks_per_game as fm_blocks_per_game
          ,fm.field_goal_percentage as fm_field_goal_percentage
          ,fm.c_3_point_field_goal_percentage as fm_c_3_point_field_goal_percentage
          ,fm.free_throw_percentage as fm_free_throw_percentage
        
    from regular r 
    left join playoffs p  on r.player_id = p.player_id and r.seasons = p.seasons 
    left join rookies ro  on r.player_id = ro.player_id
    left join first_regular_season frs  on r.player_id = frs.player_id 
    left join regular_season_mvp rsm  on r.player_id = rsm.player_id and r.seasons = rsm.seasons
    left join finals_mvp fm  on r.player_id = fm.player_id and r.seasons = fm_seasons  
)


select * 
from final_regular
