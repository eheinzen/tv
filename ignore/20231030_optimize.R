set.seed(20231030)
library(tidyverse)
expos <- tibble(
  id = paste0("A", 1:1000),
  exposure_start = as.Date("2022-10-30") + 1:1000,
  exposure_stop = exposure_start + 1 + rpois(1000, 100)
)
specs <- tibble(
  feature = paste0("feat", 100:400),
  lookback_start = 0,
  lookback_end = Inf,
  aggregation = sample(c("lvcf", "any", "ts", "count"), 301, replace = TRUE)
)
X <- tibble(
  feature = sample(specs$feature, 1e6, replace = TRUE),
  id = sample(expos$id, 1e6, replace = TRUE),
  datetime = as.Date("2022-10-30") - 100 + as.numeric(sub("A", "", id)) + rpois(1e6, 125),
  value = rnorm(1e6)
)
nrow(time_varying(X, specs, expos, time_units = "days", id = "id", grid.only = T))

t1 <- Sys.time()
tv1 <- time_varying(X, specs, expos, time_units = "days", id = "id", n_cores = 10)
t2 <- Sys.time()
print(t2 - t1)

t3 <- Sys.time()
tv2 <- time_varying2(X, specs, expos, time_units = "days", id = "id", n_cores = 10)
t4 <- Sys.time()
print(t4 - t3)


t5 <- Sys.time()
tv3 <- time_varying3(X, specs, expos, time_units = "days", id = "id", n_cores = 10)
t6 <- Sys.time()
print(t6 - t5)

identical(tv1, tv2)
identical(tv2, tv3)
