library(dplyr)

x <- data.frame(
  id = 1,
  feature = c("event", "lab", "lab", "lab", "lab"),
  datetime = as.Date(c("2023-01-02", "2022-12-01", "2022-12-15", "2022-12-31", "2023-01-02")),
  value = c(1, 1, 2, 3.3, 3)
)

exposure <- data.frame(
  id = 1,
  exposure_start = as.Date("2023-01-01"),
  exposure_stop = as.Date("2023-01-03")
)

specs <- data.frame(
  feature = c("event", "lab"),
  use_for_grid = FALSE,
  lookback_start = 0,
  lookback_end = c(0, 5),
  aggregation = c("event", "lvcf")
)

result <- data.frame(
  id = 1,
  exposure_start = as.Date("2023-01-01"),
  exposure_stop = as.Date("2023-01-03"),
  row_start = as.Date("2023-01-01"),
  row_stop = as.Date("2023-01-03"),
  event_event = 1
)


test_that("the specs work", {
  expect_equal(
    time_varying(x, specs, exposure, time_units = "days", id = "id"),
    mutate(result, lab_lvcf = 3.3),
    ignore_attr = "coltype"
  )

  expect_equal(
    time_varying(x, mutate(specs, lookback_start = 2, lookback_end = c(2, 5)), exposure, time_units = "days", id = "id"),
    mutate(result, lab_lvcf = NA_real_),
    ignore_attr = "coltype"
  )

  expect_equal(
    time_varying(x, mutate(specs, lookback_start = 2, lookback_end = c(2, 18)), exposure, time_units = "days", id = "id"),
    mutate(result, lab_lvcf = 2),
    ignore_attr = "coltype"
  )

  expect_equal(
    time_varying(x, mutate(specs, aggregation = c("event", "count")), exposure, time_units = "days", id = "id"),
    mutate(result, lab_count = 1),
    ignore_attr = "coltype"
  )

  expect_equal(
    time_varying(x, mutate(specs, aggregation = c("event", "mean"), lookback_end = c(0, Inf)), exposure, time_units = "days", id = "id"),
    mutate(result, lab_mean = 2.1),
    ignore_attr = "coltype"
  )

  expect_equal(
    time_varying(x, mutate(specs, aggregation = c("event", "median"), lookback_end = c(0, Inf)), exposure, time_units = "days", id = "id"),
    mutate(result, lab_median = 2),
    ignore_attr = "coltype"
  )

  expect_equal(
    time_varying(x, mutate(specs, aggregation = c("event", "sum"), lookback_end = c(0, Inf)), exposure, time_units = "days", id = "id"),
    mutate(result, lab_sum = 6.3),
    ignore_attr = "coltype"
  )

  expect_equal(
    time_varying(x, mutate(specs, lookback_end = c(0, NA)), exposure, time_units = "days", id = "id"),
    mutate(result, lab_lvcf = NA_real_),
    ignore_attr = "coltype"
  )

  expect_equal(
    time_varying(x, mutate(specs, lookback_start = NA), exposure, time_units = "days", id = "id"),
    mutate(result, lab_lvcf = 3.3),
    ignore_attr = "coltype"
  )

  expect_equal(
    time_varying(x, mutate(specs, lookback_start = NA, use_for_grid = TRUE), exposure, time_units = "days", id = "id"),
    data.frame(
      id = 1,
      exposure_start = as.Date("2023-01-01"),
      exposure_stop = as.Date("2023-01-03"),
      row_start = as.Date(c("2023-01-01", "2023-01-02")),
      row_stop = as.Date(c("2023-01-02", "2023-01-03")),
      event_event = c(1, 0),
      lab_lvcf = 3.3
    ),
    ignore_attr = "coltype"
  )

  expect_equal(
    time_varying(x, mutate(specs, lookback_end = NA, use_for_grid = TRUE), exposure, time_units = "days", id = "id"),
    data.frame(
      id = 1,
      exposure_start = as.Date("2023-01-01"),
      exposure_stop = as.Date("2023-01-03"),
      row_start = as.Date(c("2023-01-01", "2023-01-02")),
      row_stop = as.Date(c("2023-01-02", "2023-01-03")),
      event_event = c(1, 0),
      lab_lvcf = c(NA, 3)
    ),
    ignore_attr = "coltype"
  )
})
