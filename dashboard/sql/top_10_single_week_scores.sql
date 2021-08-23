WITH top_single_weeks AS
(
    SELECT
        ms.manager_initials
        , s.year
        , m.week
        , m.points
        , RANK() OVER(ORDER BY m.points DESC) AS score_rank
    FROM
        `robboli-broc.dbt.matchups` AS m
    LEFT JOIN
        `robboli-broc.dbt.seasons` AS s
        ON m.league_name = s.league_name
        AND m.season_id = s.season_id
    LEFT JOIN
        `robboli-broc.dbt.manager_seasons` AS ms
        ON m.league_name = ms.league_name
        AND m.season_id = ms.season_id
        AND m.roster_id = ms.roster_id
    WHERE
        m.is_median_matchup = 0
    ORDER BY
        score_rank
    LIMIT
        20
)

SELECT
    manager_initials
    , FORMAT("%'.2f", points) AS points
    , year
    , CONCAT('Week ', CAST(week AS STRING))
FROM
    top_single_weeks
WHERE
    score_rank <= 10