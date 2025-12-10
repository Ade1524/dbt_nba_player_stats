with table_players_in_the_playoffs as (
    select * 
    from {{ ref('int_nba_player_stats__playoffs_players_added_team_name') }}
)
, playoffs_players as (
    select player_id
          ,player_name
          ,year_of_birth
    from table_players_in_the_playoffs
)

select * from playoffs_players