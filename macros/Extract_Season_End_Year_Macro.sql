{% macro extract_season_end_year(season_column) %}
    {#
    Extracts the ending year (YYYY) from a season string (YYYY-YYYY format)
    and casts the result as an INTEGER.

    Example: '1949-1950' becomes 1950 (INTEGER)

    Args:
        season_column (string): The name of the column containing the season string.

    Usage example:
        SELECT {{ extract_season_end_year('seasons') }} AS season_end_year
        FROM {{ ref('dim_nba_seasons') }}
    #}
    CAST(SPLIT_PART({{ season_column }}, '-', 2) AS INTEGER)
{% endmacro %}