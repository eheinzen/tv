# tv v1.4.0

- The exposures are now expected to be non-overlapping (although windows can touch when a start time
  equals an end time).

# tv v1.3.0

- Reorganized the help pages.

- Added special behavior for when `"lookback_end"` or `"lookback_start"` are `NA`.

- The grid now includes `"exposure_start"` and `"exposure_stop"`.

- Added aggregation option `"tv_median"`.

- Added tests with `testthat`.

# tv v1.2.3

- Updated vignette.

- Forced warnings to be printed as they occur, instead of when `tv::time_varying()` finishes.

# tv v1.2.2

- Added the aggregation option `"sum"`.

# tv v1.2.1

- Fixed an error message.

# tv v1.2.0

- Added an attribute denoting what type of column each of the resulting columns is. This is useful for subsetting for, e.g., xgboost.

# tv v1.1.0

- Added `id=` argument.

# tv v1.0.0

- Initial port of code from `kernds`, removing all kds prefixes.
