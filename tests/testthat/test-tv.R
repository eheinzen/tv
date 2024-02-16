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



test_that("overlaps are detected", {
  expect_error(
    capture.output(time_varying(
      data.frame(pat_id = 1, feature = "lab", datetime = as.Date("2022-01-01"), value = 1),
      specs = data.frame(
        feature = "lab",
        use_for_grid = FALSE,
        lookback_start = 0,
        lookback_end = 10,
        aggregation = "lvcf"
      ),
      exposure = data.frame(
        pat_id = c(1, 1),
        exposure_start = as.Date(c("2022-01-01", "2022-01-02")),
        exposure_stop = as.Date(c("2022-01-03", "2022-01-04"))
      )
    )),
    "overlap"
  )

})





test_that("NA aggregations are detected", {
  expect_error(
    time_varying(x, mutate(specs, aggregation = c("lvcf", NA)), exposure, time_units = "days", id = "id"),
    "Some aggregations you supplied aren't supported: NA"
  )

})


test_that("all-NA values passed to min/max are detected and not warned", {
  expect_warning(
    out <- time_varying(
      data.frame(pat_id = 1, feature = "lab", datetime = as.Date("2022-01-01"), value = NA_real_),
      specs = data.frame(
        feature = "lab",
        use_for_grid = FALSE,
        lookback_start = 0,
        lookback_end = 10,
        aggregation = c("min", "max")
      ),
      exposure = data.frame(
        pat_id = 1,
        exposure_start = as.Date("2022-01-01"),
        exposure_stop = as.Date("2022-01-03")
      )
    ),
    NA
  )
  expect_equal(
    out[c("lab_min", "lab_max")],
    data.frame(lab_min = NA_real_, lab_max = NA_real_)
  )

})



test_that("zero-length exposures are detected", {
  expect_error(
    time_varying(
      data.frame(pat_id = 1:2, feature = "lab", datetime = as.Date("2022-01-01"), value = 1),
      specs = data.frame(
        feature = "lab",
        use_for_grid = FALSE,
        lookback_start = 0,
        lookback_end = 10,
        aggregation = "lvcf"
      ),
      exposure = data.frame(
        pat_id = c(1, 2),
        exposure_start = as.Date(c("2022-01-01", "2022-01-02")),
        exposure_stop = as.Date(c("2022-01-01", "2022-01-04"))
      )
    ),
    "There are zero- or negative-length"
  )

})



test_that("sorting is done right", {
  expect_message(
    time_varying(
      data.frame(pat_id = c(1, 1), feature = "lab", datetime = as.Date("2022-01-01"), value = c(1, 0)),
      specs = data.frame(
        feature = "lab",
        use_for_grid = FALSE,
        lookback_start = 0,
        lookback_end = 10,
        aggregation = "lvcf"
      ),
      exposure = data.frame(
        pat_id = c(1),
        exposure_start = as.Date(c("2022-01-01")),
        exposure_stop = as.Date(c("2022-01-02"))
      )
    ),
    "be sure that your data is sorted in descending datetime order"
  )
  expect_equal(
    time_varying(
      data.frame(pat_id = c(1, 1), feature = "lab", datetime = as.POSIXct(c("2022-01-01 00:00:00", "2022-01-01 00:00:01")), value = c(1, 0)),
      specs = data.frame(
        feature = "lab",
        use_for_grid = FALSE,
        lookback_start = 0,
        lookback_end = 10,
        aggregation = "lvcf"
      ),
      exposure = data.frame(
        pat_id = c(1),
        exposure_start = as.Date(c("2022-01-01")),
        exposure_stop = as.Date(c("2022-01-02"))
      )
    )$lab_lvcf,
    0
  )
  expect_equal(
    time_varying(
      data.frame(pat_id = c(1, 1), feature = "lab", datetime = as.POSIXct(c("2022-01-01 00:00:00", "2022-01-01 00:00:01")), value = c(1, 0)),
      specs = data.frame(
        feature = "lab",
        use_for_grid = FALSE,
        lookback_start = 0,
        lookback_end = 10,
        aggregation = "lvcf"
      ),
      exposure = data.frame(
        pat_id = c(1),
        exposure_start = as.Date(c("2022-01-01")),
        exposure_stop = as.Date(c("2022-01-02"))
      ),
      sort = FALSE # induce the bad behavior
    )$lab_lvcf,
    1 # !!! the wrong answer!
  )
})









