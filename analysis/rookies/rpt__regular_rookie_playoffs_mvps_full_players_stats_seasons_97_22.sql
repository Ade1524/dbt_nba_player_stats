with regular_97_22 as (
    select * 
    from {{ ref('fct__regular_rookie_playoffs_mvps_full_players_stats_seasons_97_22') }}
)

, players as (
    select *    
     from {{ ref('dim_nba_players') }}
)

, seasons as (
    select *    
     from {{ ref('dim_nba_seasons') }}
)

, teams as (
    select *    
     from {{ ref('dim_nba_teams') }}
)
, final_regular as (
     
     select s.dim_season_key
           ,s.seasons 
           ,p.dim_player_key
           ,p.player_id
           ,p.player_name
           ,p.year_of_birth
           ,t.dim_team_key
           ,t.teams
           ,t.team_name

-- Seasons 97-22 player stats columns          
          ,r.r_position
          ,r.r_age
          ,r.r_games
          ,r.r_games_started
          ,r.r_minutes_played_per_game
          ,r.r_field_goals_per_game
          ,r.r_field_goal_attempts_per_game
          ,r.r_field_goal_percentage
          ,r.r_c_3_point_field_goals_per_game
          ,r.r_c_3_point_field_goal_attempts_per_game
          ,r.r_c_3_point_field_goal_percentage
          ,r.r_c_2_point_field_goals_per_game
          ,r.r_c_2_point_field_goal_attempts_per_game
          ,r.r_c_2_point_field_goal_percentage
          ,r.r_effective_field_goal_percentage
          ,r.r_free_throws_per_game
          ,r.r_free_throw_attempts_per_game
          ,r.r_free_throw_percentage
          ,r.r_offensive_rebounds_per_game
          ,r.r_defensive_rebounds_per_game
          ,r.r_total_rebounds_per_game
          ,r.r_assists_per_game
          ,r.r_steals_per_game
          ,r.r_blocks_per_game
          ,r.r_turnovers_per_game
          ,r.r_personal_fouls_per_game
          ,r.r_points_per_game
          ,r.num_years_in_league

-- Playoffs player stats columns
          ,r.p_position
          ,r.p_age
          ,r.p_playoff_games_in_season
          ,r.p_playoff_games_started_in_season
          ,r.p_minutes_played_per_game
          ,r.p_field_goals_per_game
          ,r.p_field_goal_attempts_per_game
          ,r.p_field_goal_percentage
          ,r.p_a_3_point_field_goals_per_game
          ,r.p_a_3_point_field_goal_attempts_per_game
          ,r.p_a_3_point_field_goal_percentage
          ,r.p_a_2_point_field_goals_per_game
          ,r.p_a_2_point_field_goal_attempts_per_game
          ,r.p_a_2_point_field_goal_percentage
          ,r.p_effective_field_goal_percentage
          ,r.p_free_throws_per_game
          ,r.p_free_throw_attempts_per_game
          ,r.p_free_throw_percentage
          ,r.p_offensive_rebounds_per_game
          ,r.p_defensive_rebounds_per_game
          ,r.p_total_rebounds_per_game
          ,r.p_assists_per_game
          ,r.p_steals_per_game
          ,r.p_blocks_per_game
          ,r.p_turnovers_per_game
          ,r.p_personal_fouls_per_game
          ,r.p_points
          ,r.p_assist_percentage
          ,r.p_block_percentage
          ,r.p_box_plus_minus
          ,r.p_defensive_box_plus_minus
          ,r.p_defensive_rebounding_percentage
          ,r.p_defensive_win_share
          ,r.p_a_3_point_shot_attempts_per_field_goal_attempted
          ,r.p_free_throw_attempted_per_field_goal_attempted_percentage
          ,r.p_total_minutes_played
          ,r.p_offensive_box_plus_minus
          ,r.p_offensive_rebounding_percentage
          ,r.p_offensive_win_share
          ,r.p_player_efficiency_rating
          ,r.p_steal_percentage
          ,r.p_turnover_percentage
          ,r.p_total_rebound_percentage
          ,r.p_true_shooting_percentage
          ,r.p_usage_percentage
          ,r.p_value_over_replacement_player
          ,r.p_win_share
          ,r.p_win_share_per_48_games

--  rookies stats columns
          ,r.ro_age 
          ,r.ro_years_in_the_league 
          ,r.ro_games_played 
          ,r.ro_minutes_played 
          ,r.ro_field_goals  
          ,r.ro_field_goals_attempted 
          ,r.ro_c_3_pointers  
          ,r.ro_c_3_points_attempted 
          ,r.ro_free_throws 
          ,r.ro_free_throws_attempted 
          ,r.ro_offensive_rebound 
          ,r.ro_true_rebound 
          ,r.ro_assists 
          ,r.ro_steals 
          ,r.ro_blocks 
          ,r.ro_turnovers 
          ,r.ro_personal_fouls 
          ,r.ro_points 
          ,r.ro_c_field_goal_percentage 
          ,r.ro_c_3p_point_percentage 
          ,r.ro_free_throw_percentage 
          ,r.ro_minutes_per_game 
          ,r.ro_points_per_game 
          ,r.ro_rebounds_per_game 
          ,r.ro_assists_per_game
          ,r.ro_roty_points_won
          ,r.ro_max_amount_of_points_you_can_win
          ,r.ro_pts_won_vs_pts_max
          ,r.ro_win_total_during_the_season
          ,r.ro_lose_total_during_the_season
          ,r.ro_win_loss_percentage
          ,r.ro_team_games_behind
          ,r.ro_points_per_game_in_the_team
          ,r.ro_opponents_points_per_game
          ,r.ro_simple_rating_system

-- Regular season mvp stats columns         
          ,r.rsm_league
          ,r.rsm_age
          ,r.rsm_games
          ,r.rsm_minutes_played_per_games
          ,r.rsm_points_per_game
          ,r.rsm_total_rebounds_per_game
          ,r.rsm_assists_per_game
          ,r.rsm_steals_per_game
          ,r.rsm_blocks_per_game
          ,r.rsm_field_goal_percentage
          ,r.rsm_c_3_point_field_goal_percentage
          ,r.rsm_free_throw_percentage
          ,r.rsm_win_share
          ,r.rsm_win_share_per_48_games
          ,r.rsm_voting
-- Final MVP
          ,r.fm_league
          ,r.fm_age
          ,r.fm_games
          ,r.fm_minutes_played_per_game
          ,r.fm_points_per_game
          ,r.fm_total_rebounds_per_game
          ,r.fm_assists_per_game
          ,r.fm_steals_per_game
          ,r.fm_blocks_per_game
          ,r.fm_field_goal_percentage
          ,r.fm_c_3_point_field_goal_percentage
          ,r.fm_free_throw_percentage
        
      from regular_97_22 r                                      
      left join players p  on r.dim_player_key = p.dim_player_key
      left join seasons s  on r.dim_season_key = s.dim_season_key
      left join teams t  on r.dim_team_key = t.dim_team_key
    
)


select * from final_regular

