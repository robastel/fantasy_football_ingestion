WITH records_with_rank AS
(
    SELECT
        manager_initials
        , first_place_count
        , second_place_count
        , third_place_count
        , FORMAT("%'.3f", made_playoffs_rate) AS made_playoffs_rate
        , FORMAT("%'.3f", regular_season_win_rate) AS regular_season_win_rate
        , season_count
        , RANK() OVER(ORDER BY
            first_place_count DESC
            , second_place_count DESC
            , third_place_count DESC
          ) AS all_time_rank
    FROM
        `robboli-broc.dbt.records`
)

SELECT
    manager_initials
    , first_place_count
    , second_place_count
    , third_place_count
    , made_playoffs_rate
    , regular_season_win_rate
    , season_count
FROM
    records_with_rank
ORDER BY
    first_place_count DESC
    , second_place_count DESC
    , third_place_count DESC
    , made_playoffs_rate DESC
    , regular_season_win_rate DESC
    , season_count DESC