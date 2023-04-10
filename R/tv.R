#' Create a time-varying dataset
#'
#' @param x A data.frame with four columns: <id>, "feature", "datetime", "value"
#' @param specs a data.frame with four columns: "feature", "use_for_grid", "lookback_start", "lookback_end", "aggregation". See details below.
#' @param exposure a data.frame with (at least) three columns: <id>, "exposure_start", "exposure_stop"
#' @param grid.only Should just the grid be computed and returned? Useful only for debugging
#' @param time_units What time units should be used? Seconds or days
#' @param n_cores Number of cores to use. If slurm is being used, it checks the \code{SLURM_CPUS_PER_TASK} variable.
#'   Else it defaults to 1, for no parallelization.
#' @param ... Other arguments. Currently just passes \code{check_overlap}.
#' @param expected_features A vector of expected features based on the data.
#' @param expected_ids A vector of expected ids based on the data.
#' @param id The id to use. Default is "pat_id"
#' @param check_overlap Should overlap be checked among exposure rows? A potentially costly operation,
#'   so you can opt out of it if you're really sure.
#' @details
#'   The defaults for specs are to use everything for the grid creation, and to set \code{lookback_start=0}, with a message in both cases.
#'   Currently supported aggregation functions include counting ("count" or "n"), last-value-carried forward ("last value" or "lvcf"),
#'   any/none ("any" or "binary"), time since ("time since" or "ts"), min/max/mean, and the special "event" (for which lookbacks are ignored).
#'
#'   The lookback window begins at \code{row_start - lookback_end} and ends at \code{row_start - lookback_start}. Passing NA to either lookback
#'   changes the corresponding window boundary to \code{exposure_start}.
#' @examples
#'   data(tv_example)
#'   time_varying(tv_example$data, tv_example$specs, tv_example$exposure,
#'                time_units = "days", id = "mcn")
#' @name tv
NULL

#' @rdname tv
#' @export
time_varying <- function(x, specs, exposure, ...,
                         grid.only = FALSE,
                         time_units = c("days", "seconds"), id = "pat_id",
                         n_cores = as.numeric(Sys.getenv("SLURM_CPUS_PER_TASK", 1))) {
  opts <- options(warn = 1)
  on.exit(options(opts))
  time_units <- match.arg(time_units)
  x <- check_tv_data(x, time_units = time_units, id = id)
  specs <- check_tv_specs(specs, unique(x$feature))
  exposure <- check_tv_exposure(exposure, expected_ids = unique(x[[id]]), time_units = time_units, id = id, ...)

  grid <- x %>%
    dplyr::filter(.data$feature %in% specs$feature[specs$use_for_grid]) %>%
    dplyr::bind_rows(dplyr::mutate(dplyr::select(exposure, dplyr::all_of(id), datetime = "exposure_start"), feature = "exposure_start")) %>%
    dplyr::distinct(.data[[id]], row_start = .data$datetime) %>%  # could use unique here, but distinct() is faster
    dplyr::left_join(x = exposure, by = id, relationship = "many-to-many") %>%
    dplyr::filter(.data$exposure_start <= .data$row_start, .data$row_start < .data$exposure_stop) %>%
    dplyr::arrange(.data[[id]], .data$row_start) %>%
    dplyr::group_by(.data[[id]]) %>%
    dplyr::mutate(
      row_stop = pmin(dplyr::lead(.data$row_start, 1), .data$exposure_stop, na.rm = TRUE)
    ) %>%
    dplyr::ungroup()

  if(grid.only) return(grid)

  FN <- if(n_cores > 1) parallel::mclapply else lapply

  x3 <- x %>%
    dplyr::mutate(
      datetime = as.numeric(.data$datetime)
    ) %>%
    (function(xxx) split(xxx, xxx[[id]]))
  specs3 <- split(specs, seq_len(nrow(specs)))

  out <- FN(seq_len(nrow(grid)), mc.cores = n_cores, function(i, ...) {
    curr_grid <- grid[i, ]
    curr_grid$row_start <- as.numeric(curr_grid$row_start)
    curr_grid$row_stop <- as.numeric(curr_grid$row_stop)
    curr_grid$exposure_start <- as.numeric(curr_grid$exposure_start)
    curr_grid$exposure_stop <- as.numeric(curr_grid$exposure_stop)
    y <- x3[[as.character(curr_grid[[id]])]]

    tmp <- vapply(seq_len(nrow(specs)), function(s) {
      curr_spec <- specs3[[s]]
      FUN <- switch(
        curr_spec$aggregation,
        count = tv_count,
        lvcf = tv_lvcf,
        ts = tv_ts,
        any = tv_any,
        max = tv_max,
        min = tv_min,
        mean = tv_mean,
        median = tv_median,
        sum = tv_sum,
        event = tv_count
      )
      idx <- y$feature == curr_spec$feature
      if(curr_spec$aggregation == "event") {
        idx <- idx & y$datetime > curr_grid$row_start & y$datetime <= curr_grid$row_stop
      } else {
        if(is.na(curr_spec$lookback_start)) {
          idx <- idx & y$datetime <= curr_grid$exposure_start
        } else {
          idx <- idx & y$datetime <= curr_grid$row_start - curr_spec$lookback_start
        }

        if(is.na(curr_spec$lookback_end)) {
          idx <- idx & y$datetime >= curr_grid$exposure_start
        } else {
          idx <- idx & y$datetime >= curr_grid$row_start - curr_spec$lookback_end
        }
      }
      FUN(y[idx, ], feature = curr_spec$feature, current_time = curr_grid$row_start)
    }, NA_real_)
    tmp <- matrix(tmp, nrow = 1)
    colnames(tmp) <- paste0(specs$feature, "_", specs$aggregation)
    tmp
  })
  out2 <- as.data.frame(do.call(rbind, out))
  stopifnot(ncol(out2) == nrow(specs))
  out3 <- cbind(grid, out2)
  attr(out3, "coltype") <- c(rep_len("grid", ncol(grid)), dplyr::if_else(specs$aggregation == "event", "event", "feature"))
  out3
}


#' @rdname tv
#' @export
check_tv_data <- function(x, time_units, id) {
  cols <- c(id, "feature", "datetime", "value")
  if(!all(cols %in% names(x))) {
    stop("The data is missing some columns: ", paste0(setdiff(cols, names(x)), collapse = ", "))
  }
  if(time_units == "days") {
    if(!lubridate::is.Date(x$datetime)) stop("You specified time_units = 'days', but x$datetime is not a Date")
  } else {
    if(!lubridate::is.POSIXct(x$datetime)) stop("You specified time_units = 'seconds', but x$datetime is not a POSIXct.")
  }
  idx <- is.na(x$datetime)
  if(any(idx)) {
    warning("There are ", sum(idx), " missing times being removed from the data.")
    x <- x[!idx, ]
  }

  x
}

#' @rdname tv
#' @export
check_tv_exposure <- function(x, expected_ids, time_units, id, ..., check_overlap = TRUE) {
  cols <- c(id, "exposure_start", "exposure_stop")
  if(!all(cols %in% names(x))) {
    stop("'exposure' is missing some columns: ", paste0(setdiff(cols, names(x)), collapse = ", "))
  }
  if(!all(expected_ids %in% x[[id]])) {
    stop("Some ids are in the data but not 'exposure'")
  }
  if(!all(x[[id]] %in% expected_ids)) {
    stop("Some exposure ids are not in the data")
  }
  if(time_units == "days") {
    if(!lubridate::is.Date(x$exposure_start) || !lubridate::is.Date(x$exposure_stop)) stop("You specified time_units = 'days', but exposure start or stop are not a Date")
  } else {
    if(!lubridate::is.POSIXct(x$exposure_start) || !lubridate::is.POSIXct(x$exposure_stop)) stop("You specified time_units = 'seconds', but exposure start or stop is not a POSIXct.")
  }

  idx <- is.na(x$exposure_start) | is.na(x$exposure_stop)
  if(any(idx)) {
    warning("There are ", sum(idx), " missing times being removed from the exposures.")
    x <- x[!idx, ]
  }

  if(check_overlap) {
    y <- dplyr::mutate(x, .exposure.row = dplyr::row_number())
    z <- dplyr::inner_join(y, y, by = id, relationship = "many-to-many") %>%
      dplyr::filter(
        .data$.exposure.row.x < .data$.exposure.row.y,
        .data$exposure_start.y < .data$exposure_stop.x,
        .data$exposure_stop.y > .data$exposure_start.x
      )
    if(nrow(z) > 0) {
      print(z)
      stop("There are overlaps in the exposure times. `time_varying()` will not return what you expect. ",
           "If you are really sure you want to proceed, use the `check_overlap=FALSE` option.")
    }
  }

  x
}



#' @rdname tv
#' @export
check_tv_specs <- function(specs, expected_features = NULL) {
  `%nin%` <- Negate(`%in%`)
  if(any(specs$feature %nin% expected_features)) {
    warning("Some features in 'specs' do not appear in the data:\n", paste0(setdiff(specs$feature, expected_features), collapse = ", "))
  }
  if(any(expected_features %nin% specs$feature)) {
    stop("Some features in the data do not appear in 'specs':\n", paste0(setdiff(expected_features, specs$feature), collapse = ", "))
  }

  if("lookback_end" %nin% names(specs)) stop("Please specify a 'lookback_end' column in 'specs'")
  if("lookback_start" %nin% names(specs)) {
    message("Setting 'lookback_start' to 0")
    specs$lookback_start <- 0
  }
  if(any(specs$lookback_end < specs$lookback_start, na.rm = TRUE)) stop("Some lookback_end's are smaller than lookback_start's")
  if("aggregation" %nin% names(specs)) stop("Please specify an aggregation column in 'specs'")
  specs_map <- c(
    count = "count",
    n = "count",
    `last value` = "lvcf",
    lvcf = "lvcf",
    `time since` = "ts",
    ts = "ts",
    any = "any",
    binary = "any",
    min = "min",
    max = "max",
    mean = "mean",
    median = "median",
    sum = "sum",
    event = "event"
  )

  bad <- setdiff(specs$aggregation, names(specs_map))
  if(length(bad)) stop("Some aggregations you supplied aren't supported: ", paste0(bad, collapse = ", "))
  specs$aggregation <- specs_map[specs$aggregation]

  if("use_for_grid" %nin% names(specs)) {
    message("Using all features to construct the grid...\n")
    specs$use_for_grid <- TRUE
  } else if(!is.logical(specs$use_for_grid)) stop("specs$use_for_grid must be a logical")

  specs
}
