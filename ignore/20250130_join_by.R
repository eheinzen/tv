set.seed(20250130)
library(tidyverse)
y <- tibble(
  id = rpois(100000, 1000000),
  exposure_start = force_tz(Sys.Date(), "America/Chicago") + 100*dhours(rnorm(100000)),
  exposure_stop = exposure_start + 0.1
)
xx <- y %>%
  select(id, datetime = exposure_start) %>%
  mutate(value = rnorm(n()), feature = "feat")
ss <- tibble(
  feature = "feat", aggregation = "lvcf",
  lookback_end = dhours(2), use_for_grid = TRUE
)

t1 <- Sys.time()
hi <- tryCatch(tv::time_varying(xx, specs = ss, exposure = y, id = "id", time_units = "seconds"), error = function(e) 1)
t2 <- Sys.time()
t2 - t1

