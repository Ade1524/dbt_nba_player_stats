-- Seasons

{% docs seasons %} Nba championship seasons (e.g., 2022-23). {% enddocs %}
{% docs dim_season_key %} Unique surrogate key identifying each NBA season. {% enddocs %}


-- Players

{% docs dim_player_key %} Surrogate key uniquely identifying each NBA player. {% enddocs %}
{% docs player_id %} Unique identifier for the NBA player. {% enddocs %}
{% docs player_name %} Full player name. {% enddocs %}
{% docs year_of_birth %} Birth year of the player used to generate player_id. {% enddocs %}
{% docs position %} Primary season position. {% enddocs %}
{% docs age %} Player age in that season. {% enddocs %}



-- Teams

{% docs dim_team_key %} Unique surrogate key identifying each NBA team. {% enddocs %}
{% docs teams %} Team abbreviation or code. {% enddocs %}
{% docs team_name %} Full official name of the NBA team. {% enddocs %}
{% docs champion_team %} Name of the team that won the championship. {% enddocs %}
{% docs runner_up__team %} Name of the team that lost in the finals. {% enddocs %}