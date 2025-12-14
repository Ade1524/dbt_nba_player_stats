with f as (
  select *
  from {{ ref('fct__regular_rookie_playoffs_mvps_full_players_stats_seasons_97_22') }}
),

era_map as (
  select *,
    case
      when substring(seasons,1,4)::int between 1997 and 1999 then 'late_90s'
      when substring(seasons,1,4)::int between 2000 and 2009 then '2000s'
      when substring(seasons,1,4)::int between 2010 and 2019 then '2010s'
      when substring(seasons,1,4)::int >= 2020 then '2020s' end as era
  from f
),

player_score as (
  select
    player_id,
    player_name,
    era,
    avg(r_points_per_game) as avg_ppg,
    avg(r_assists_per_game) as avg_apg,
    avg(r_total_rebounds_per_game) as avg_rpg,
    avg((coalesce(r_points_per_game,0)*1.0 + coalesce(r_assists_per_game,0)*1.5 + coalesce(r_total_rebounds_per_game,0)*1.2)) as composite_score
  from era_map
  group by player_id,
           player_name,
           era
)

select era, 
       player_id, 
       player_name, 
       composite_score, 
       avg_ppg, 
       avg_apg, 
       avg_rpg
from (
  select 
        *, 
        row_number() over(partition by era order by composite_score desc) as rn
  from player_score
) t
where rn <= 10
order by era, 
         composite_score desc
