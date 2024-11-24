with player_stats_97_22_no_tot_row as (
    select *
     from {{ ref('int_nba_player_stats__players_97_22_with_added_team_name_column')}}
)

, rookie_players_stats as (
    select *
      from {{ ref('stg_nba_player_stats__nba_total_rookies_stats_1980_2023')}}
)

, union_rookie_player_stats_id as (
    select 
           player_id
          ,player_name
          ,year_of_birth
      from player_stats_97_22_no_tot_row
    union 
    select player_id
          ,player_name
          ,year_of_birth
      from rookie_players_stats  
           
)

select * from  union_rookie_player_stats_id





