{% macro extract_season_start_year(season_column) %}
    {#
    Extracts the starting year (YYYY) from a season string (YYYY-YYYY format)
    and casts the result as an INTEGER.

    Example: '1949-1950' becomes 1949 (INTEGER)

    Args:
        season_column (string): The name of the column containing the season string.

    Usage example:
        SELECT {{ extract_season_start_year('seasons') }} AS season_start_year
        FROM {{ ref('dim_nba_seasons') }}
    #}
    CAST(SPLIT_PART({{ season_column }}, '-', 1) AS INTEGER)
{% endmacro %}