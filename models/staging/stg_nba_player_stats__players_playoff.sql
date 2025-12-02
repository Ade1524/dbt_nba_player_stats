with nba_playoff_stats__players as (
    select * from {{ ref('nba_playoff_stats')}}
)

, renaming_playoff_players_columns as  (
          select {{ get_season('season') }} as seasons
                ,replace(player, '-', ' ')::varchar(1000) as player_name
                ,pos::varchar(1000) as position
                ,{{ get_year_of_birth('season', 'age') }} as year_of_birth
                ,age::int as age
                ,replace(replace(replace(team_id, 'CHO', 'CHH'), 'BRK', 'BKN'), 'MNL', 'MPL') as teams
                ,g::int as playoff_games_in_season
                ,gs::int as playoff_games_started_in_season
                ,mp_per_g::float as minutes_played_per_game
                ,fg_per_g::float as field_goals_per_game
                ,fga_per_g::float as field_goal_attempts_per_game
                ,fg_pct::float as field_goal_percentage
                ,fg3_per_g::float as a_3_point_field_goals_per_game
                ,fg3a_per_g::float as a_3_point_field_goal_attempts_per_game
                ,fg3_pct::float as a_3_point_field_goal_percentage
                ,fg2_per_g::float as a_2_point_field_goals_per_game
                ,fg2a_per_g::float as a_2_point_field_goal_attempts_per_game
                ,fg2_pct::float as a_2_point_field_goal_percentage
                ,efg_pct::float as effective_field_goal_percentage
                ,ft_per_g::float as free_throws_per_game
                ,fta_per_g::float as free_throw_attempts_per_game
                ,ft_pct::float as free_throw_percentage
                ,orb_per_g::float as offensive_rebounds_per_game
                ,drb_per_g::float as defensive_rebounds_per_game
                ,trb_per_g::float as total_rebounds_per_game
                ,ast_per_g::float as assists_per_game
                ,stl_per_g::float as steals_per_game
                ,blk_per_g::float as blocks_per_game
                ,tov_per_g::float as turnovers_per_game
                ,pf_per_g::float as  personal_fouls_per_game
                ,pts_per_g::float as points
                ,ast_pct::float as assist_percentage
                ,blk_pct::float as block_percentage
                ,bpm::float as box_plus_minus
                ,dbpm::float as defensive_box_plus_minus
                ,drb_pct::float as defensive_rebounding_percentage
                ,dws::float as defensive_win_share
                ,fg3a_per_fga_pct::float as a_3_point_shot_attempts_per_field_goal_attempted
                ,fta_per_fga_pct::float as free_throw_attempted_per_field_goal_attempted_percentage
                ,mp::int as total_minutes_played
                ,obpm::float as offensive_box_plus_minus
                ,orb_pct::float as offensive_rebounding_percentage
                ,ows::float as offensive_win_share
                ,per::float as player_efficiency_rating
                ,stl_pct::float as steal_percentage
                ,tov_pct::float as turnover_percentage
                ,trb_pct::float as total_rebound_percentage
                ,ts_pct::float as true_shooting_percentage
                ,usg_pct::float as usage_percentage
                ,vorp::float as value_over_replacement_player
                ,ws::float as win_share
                ,ws_per_48::float as  win_share_per_48_games

            from nba_playoff_stats__players
)

,playoff_players_stats as (
    select  {{ get_player_id('player_name', 'year_of_birth')}} as player_id 
            , *
    from renaming_playoff_players_columns
    order by seasons
)

select * from playoff_players_stats



