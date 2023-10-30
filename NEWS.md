# tv v2.0.0

- Completely overhauled the internals, for speeds boosts of up to 6x-8x.

# tv v1.7.3

- Added return values to the documentation and resubmitted to CRAN.

# tv v1.7.2

- Submit to CRAN.

# tv v1.7.1

- Added logic to `tv_min()`, `tv_max()`, et al. to handle cases where there are
  relevant rows for the feature, but which are all missing. Usually you will want
  to filter these NA rows out *a priori*, because they're almost never what you want.

# tv v1.7.0

- Added `grid.only=` argument to `time_varying()`.

- Thank you, `dplyr` for changing your minds. Enforce `dplyr (>= 1.1.1)`.

- Added a "How does this actually work" section to the vignette.

# tv v1.6.1

- Added `multiple = 'all'` to one `dplyr::inner_join()` and one `dplyr::left_join()`.

# tv v1.6.0

- Aggregations are now checked to make sure they exist. This includes, in particular, when the aggregation is `NA`.

- Removed a few imports from the DESCRIPTION that weren't being used.

# tv v1.5.0

- Changed the default number of cores to check if `"SLURM_CPUS_PER_TASK"` is defined.

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
