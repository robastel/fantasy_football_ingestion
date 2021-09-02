WITH top_single_weeks AS
(
    SELECT
        manager_initials
        , year
        , week
        , points
        , RANK() OVER(ORDER BY points DESC) AS score_rank
    FROM
        robboli-broc.fantasy_football.matchups
    WHERE
        is_median_matchup = 0
    ORDER BY
        score_rank
    LIMIT
        20
)

SELECT
    manager_initials
    , FORMAT("%'.2f", points) AS points
    , CONCAT(',', CAST(year AS STRING))
    , CONCAT(',Week', CAST(week AS STRING))
FROM
    top_single_weeks
WHERE
    score_rank <= 10