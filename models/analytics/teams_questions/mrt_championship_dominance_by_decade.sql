-- 2️⃣ How has championship dominance shifted across decades (1940s → 2020s)?

with dim_teams as (
    select *
    from {{ ref('dim_nba_seasons') }}
)


, new_dim_season as (
    select
        *, -- Selects all existing columns first
    -- Use the existing macro for the first year
        {{ extract_season_start_year('seasons') }} AS season_start_year, -- Result: 1949 (INTEGER)
    -- Use the new macro for the second year
        {{ extract_season_end_year('seasons') }} AS season_end_year      -- Result: 1950 (INTEGER)
    from
    dim_teams
)

, decade_map as (
    select
        *,
        case
            when season_start_year between 1947 and 1959 then '1950s'
            when season_start_year between 1960 and 1969 then '1960s'
            when season_start_year between 1970 and 1979 then '1970s'
            when season_start_year between 1980 and 1989 then '1980s'
            when season_start_year between 1990 and 1999 then '1990s'
            when season_start_year between 2000 and 2009 then '2000s'
            when season_start_year between 2010 and 2019 then '2010s'
            when season_start_year between 2020 and 2029 then '2020s'
        end as decade
    from new_dim_season
)


select
    decade,
    champions as franchise,
    count(*) as titles,
    round(count(*) * 100.0 / sum(count(*)) over(partition by decade), 2) as pct_of_decade_titles
from decade_map
group by 1, 2
order by decade, titles desc

