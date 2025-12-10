with mvp_player_season as (
    select * 
    from {{ ref('nba_season_mvp')}}
)

,rename_columns_mvp_players_stats as (
    select 
           concat((substring(season,1,4))::varchar,'-',(substring(season,1,4)::int + 1)::varchar) as seasons
          ,lg as league
          ,replace(player, '-', ' ')::varchar(1000) as player_name
          ,substring(season,1,4)::int - age as year_of_birth
          ,age
          ,tm::varchar(1000) as teams
          ,g::int as games
          ,mp::float as minutes_played_per_games
          ,pts::float as points_per_game
          ,trb::float as total_rebounds_per_game
          ,ast::float as assists_per_game
          ,stl::float as steals_per_game
          ,blk::float as blocks_per_game
          ,fg_pct::float as field_goal_percentage
          ,c_3p_pct::float as c_3_point_field_goal_percentage
          ,ft_pct::float as free_throw_percentage
          ,ws::float as win_share
          ,ws_48::float as  win_share_per_48_games
          ,voting ::varchar(1000) as voting
    from mvp_player_season
)

,mvp_season_id as (
        select  
             {{ get_player_id('player_name', 'year_of_birth')}} as player_id 
            , *
        from rename_columns_mvp_players_stats 
)


select * 
from mvp_season_id
