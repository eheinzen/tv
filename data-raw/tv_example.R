## code to prepare `tv_example` dataset goes here
library(tidyverse)
tv_example <- list(
  data = tribble(
    ~ mcn, ~ feature, ~ datetime, ~ value,
    1, "albumin", "2022-01-01", 15,
    1, "neurosurgery", "2022-01-31", NA,
    1, "neuro note", "2022-02-06", NA,
    1, "neuro note", "2022-02-07", NA,
    1, "neuro appointment", "2022-02-07", NA,
    2, "albumin", "2022-02-02", 12,
    2, "death", "2022-02-03", NA
  ) %>%
    mutate(datetime = lubridate::as_date(datetime)),

  specs = tribble(
    ~ feature, ~ use_for_grid, ~ lookback_start, ~ lookback_end, ~ aggregation,
    "albumin", TRUE, 0, 31, "last value",
    "albumin", TRUE, 0, 31, "time since",
    "neurosurgery", TRUE, 0, 365, "count",
    "neuro note", TRUE, 0, 365, "binary",
    "neuro appointment", TRUE, 0, 30, "count",
    "neuro appointment", TRUE, 0, 30, "time since",
    "death", TRUE, 0, 0, "event"
  ),

  exposure = tribble(
    ~ mcn, ~ exposure_start, ~ exposure_stop,
    1, "2022-02-01", "2022-02-08",
    2, "2022-02-01", "2022-02-08"
  ) %>%
    mutate(
      exposure_start = lubridate::as_date(exposure_start),
      exposure_stop = lubridate::as_date(exposure_stop)
    )
)

usethis::use_data(tv_example, overwrite = TRUE)
