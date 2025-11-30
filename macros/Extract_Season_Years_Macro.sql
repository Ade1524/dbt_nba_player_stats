{% macro extract_season_years(season_column) %}
    {#
    Generates two INTEGER columns: season_start_year and season_end_year,
    by splitting the season string (YYYY-YYYY format).

    Example usage:
        SELECT
            *,
            {{ extract_season_years('seasons') }}
        FROM {{ ref('dim_nba_seasons') }}

    This expands to:
        SELECT
            *,
            CAST(SPLIT_PART(seasons, '-', 1) AS INTEGER) AS season_start_year,
            CAST(SPLIT_PART(seasons, '-', 2) AS INTEGER) AS season_end_year
        FROM ...
    
    Args:
        season_column (string): The name of the column containing the season string.
    #}

    CAST(SPLIT_PART({{ season_column }}, '-', 1) AS INTEGER) AS season_start_year,
    CAST(SPLIT_PART({{ season_column }}, '-', 2) AS INTEGER) AS season_end_year

{% endmacro %}