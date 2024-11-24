with nba_player_stats as (
    select * from {{ ref('nba_player_stats')}}
)

, rename_player_stats_columns as (
      select rk as rank_id
            ,year::varchar(1000) as seasons
            ,{{ clean_player_name('player') }} as player_name
            ,substring(year,1,4)::int - age as year_of_birth
            ,pos::varchar(1000) as position
            ,age::int as age
            ,replace(replace(tm, 'CHO', 'CHH'), 'BRK', 'BKN') as teams
            ,g::int as games
            ,gs::int as games_started
            ,mp::float as minutes_played_per_game
            ,fg::float as field_goals_per_game
            ,fga::float as field_goal_attempts_per_game
            ,fg_pct::float as field_goal_percentage
            ,c_3p::float as c_3_point_field_goals_per_game
            ,c_3pa::float as  c_3_point_field_goal_attempts_per_game
            ,c_3p_pct::float as c_3_point_field_goal_percentage
            ,c_2p::float as c_2_point_field_goals_per_game
            ,c_2pa::float as c_2_point_field_goal_attempts_per_game
            ,c_2p_pct::float as c_2_point_field_goal_percentage
            ,efg_pct::float as effective_field_goal_percentage
            ,ft::float as free_throws_per_game
            ,fta::float as  free_throw_attempts_per_game
            ,ft_pct::float as free_throw_percentage
            ,orb::float as  offensive_rebounds_per_game
            ,drb::float as defensive_rebounds_per_game
            ,trb::float as total_rebounds_per_game
            ,ast::float as assists_per_game
            ,stl::float as steals_per_game
            ,blk::float as blocks_per_game
            ,tov::float as turnovers_per_game
            ,pf::float as personal_fouls_per_game
            ,pts::float as points_per_game
            
        from nba_player_stats
)

,players_stats_per_season as (
    select  {{ get_player_id('player_name', 'year_of_birth')}} as player_id 
            , *
      from rename_player_stats_columns
)

select * from players_stats_per_season



