#' Create a time-varying dataset
#'
#' @param x A data.frame with four columns: <id>, "feature", "datetime", "value"
#' @param specs a data.frame with four columns: "feature", "use_for_grid", "lookback_start", "lookback_end", "aggregation". See details below.
#' @param exposure a data.frame with (at least) three columns: <id>, "exposure_start", "exposure_stop"
#' @param grid_only Should just the grid be computed and returned? Useful only for debugging
#' @param time_units What time units should be used? Seconds or days
#' @param n_cores Number of cores to use. If slurm is being used, it checks the \code{SLURM_CPUS_PER_TASK} variable.
#'   Else it defaults to 1, for no parallelization.
#' @param ... Other arguments. Currently just passes \code{check_overlap}.
#' @param expected_features A vector of expected features based on the data.
#' @param expected_ids A vector of expected ids based on the data.
#' @param id The id to use. Default is "pat_id"
#' @param sort Logical, indicating whether to sort the data before performing the analysis. By default (\code{NA}),
#'   sorting is only done when useful (that is: \code{x$datetime} is a POSIXct and \code{time_units == "days"}).
#'   A warning is issued when \code{x$datetime} is a Date to make the user aware that the input ought to be sorted to
#'   get the right answer.
#' @param check_overlap Should overlap be checked among exposure rows? A potentially costly operation,
#'   so you can opt out of it if you're really sure.
#' @return A data.frame, with one row per grid value and one column per feature specification (plus grid columns).
#' @details
#'   The defaults for specs are to use everything for the grid creation, and to set \code{lookback_start=0}, with a message in both cases.
#'   Currently supported aggregation functions include counting ("count" or "n"), last-value-carried forward ("last value" or "lvcf"),
#'   any/none ("any" or "binary"), time since ("time since" or "ts"), min/max/mean, and the special "event" (for which look backs are ignored).
#'
#'   The look back window begins at \code{row_start - lookback_end} and ends at \code{row_start - lookback_start}. Passing NA to either look back
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
                         grid_only = FALSE,
                         time_units = c("days", "seconds"), id = "pat_id", sort = NA,
                         n_cores = parallelly::availableCores(omit = 1)) {
  opts <- options(warn = 1)
  on.exit(options(opts))
  ignore <- function() parallelly::availableCores # so devtools::check finds it

  time_units <- match.arg(time_units)
  x <- check_tv_data(x, time_units = time_units, id = id, sort = sort)
  specs <- check_tv_specs(specs, unique(x$feature))
  exposure <- check_tv_exposure(exposure, expected_ids = unique(x[[id]]), time_units = time_units, id = id, ...)
  exposure$..tv.id.. <- exposure[[id]]

  grid <- x %>%
    dplyr::filter(.data$feature %in% specs$feature[specs$use_for_grid]) %>%
    dplyr::select(all_of(c("..tv.id.." = id)), "datetime") %>%
    dplyr::bind_rows(dplyr::select(exposure, ..tv.id.., datetime = "exposure_start")) %>%
    dplyr::distinct(..tv.id.., row_start = .data$datetime) %>%  # could use unique here, but distinct() is faster
    dplyr::left_join(
      x = exposure, relationship = "many-to-many",
      by = dplyr::join_by(
        x$..tv.id.. == y$..tv.id..,
        x$exposure_start <= y$row_start,
        y$row_start < x$exposure_stop
      )
    ) %>%
    dplyr::select(-"..tv.id..") %>%
    dplyr::arrange(.data[[id]], .data$row_start) %>%
    dplyr::group_by(.data[[id]]) %>%
    dplyr::mutate(
      row_stop = dplyr::lead(.data$row_start, 1)
    ) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(
      row_stop = pmin(.data$row_stop, .data$exposure_stop, na.rm = TRUE)
    )

  if(grid_only) return(grid)

  FN <- if(n_cores > 1) parallel::mcMap else Map

  g2 <- grid %>%
    dplyr::mutate(
      dplyr::across(c("row_start", "row_stop", "exposure_start", "exposure_stop"), as.numeric),
      ..tv.row.. = dplyr::row_number()
    ) %>%
    (function(xxx) split(xxx, xxx[[id]]))

  revert <- order(do.call(c, lapply(g2, "[[", "..tv.row..")))

  x3 <- x %>%
    dplyr::mutate(
      datetime = as.numeric(.data$datetime)
    ) %>%
    (function(xxx) split(xxx, xxx[[id]]))

  stopifnot(
    length(g2) == length(x3),
    names(g2) == names(x3)
  )

  specs3 <- split(specs, seq_len(nrow(specs)))

  out <- FN(x3, g2, mc.cores = n_cores, f = function(xx, gg, ...) {
    gg <- as.matrix(gg[c("exposure_start", "row_start", "row_stop")])
    xfeat <- xx$feature
    yy <- as.matrix(xx[c("value", "datetime")])

    tmp <- vapply(specs3, FUN.VALUE = numeric(nrow(gg)), USE.NAMES = FALSE, function(curr_spec) {
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
        event = tv_count,
        error = stop
      )
      curr_feat <- curr_spec$feature
      y <- yy[xfeat == curr_feat, , drop = FALSE]
      val <- y[, 1]
      dttm <- y[, 2]
      if(nrow(y) == 0) {
        return(rep.int(FUN(value = val, datetime = dttm, current_time = stop("Shouldn't need this")), nrow(gg)))
      }

      curr_lookback_start <- curr_spec$lookback_start
      curr_lookback_end <- curr_spec$lookback_end
      curr_aggregation <- curr_spec$aggregation
      idx.true <- rep.int(TRUE, nrow(y))
      apply(gg, 1, function(curr_grid) {
        curr_row_start <- curr_grid[2]
        idx <- idx.true
        if(curr_aggregation == "event") {
          idx <- idx & dttm > curr_row_start & dttm <= curr_grid[3]
        } else {
          if(is.na(curr_lookback_start)) {
            idx <- idx & dttm <= curr_grid[1]
          } else {
            idx <- idx & dttm <= curr_row_start - curr_lookback_start
          }

          if(is.na(curr_lookback_end)) {
            idx <- idx & dttm >= curr_grid[1]
          } else {
            idx <- idx & dttm >= curr_row_start - curr_lookback_end
          }
        }
        FUN(value = val[idx], datetime = dttm[idx], current_time = curr_row_start)
      })
    })
    tmp
  })
  idx <- vapply(out, inherits, NA, what = "try-error")
  if(any(idx)) {
    stop("Errors were detected. The first one is: ", out[idx][[1]])
  }

  out <- do.call(rbind, out)
  dimnames(out) <- list(NULL, paste0(specs$feature, "_", specs$aggregation))
  out2 <- as.data.frame(out[revert, , drop = FALSE])
  stopifnot(ncol(out2) == nrow(specs))
  out3 <- cbind(grid, out2)
  attr(out3, "coltype") <- c(rep_len("grid", ncol(grid)), dplyr::if_else(specs$aggregation == "event", "event", "feature"))
  out3
}


#' @rdname tv
#' @export
check_tv_data <- function(x, time_units, id, sort) {
  cols <- c(id, "feature", "datetime", "value")
  if(!all(cols %in% names(x))) {
    stop("The data is missing some columns: ", paste0(setdiff(cols, names(x)), collapse = ", "))
  }
  if(time_units == "days") {
    if(!lubridate::is.POSIXct(x$datetime) && !lubridate::is.Date(x$datetime)) {
      stop("x$datetime needs to be a Date or POSIXct.")
    } else if(lubridate::is.Date(x$datetime) && is.na(sort)) {
      message("x$datetime is a Date; as such, be sure that your data is sorted in descending datetime order, so that `lvcf` picks the most recent row correctly",
              " (it picks the first row it finds).\n\n",
              "To silence this message, please specify `sort=TRUE` or `sort=FALSE`. Defaulting to `sort=FALSE`.")
      sort <- FALSE
    } else if(is.na(sort)) {
      # datetime
      sort <- TRUE
    }
  } else {
    if(!lubridate::is.POSIXct(x$datetime)) stop("You specified time_units = 'seconds', but x$datetime is not a POSIXct.")
    if(is.na(sort)) sort <- FALSE
  }

  idx <- is.na(x$datetime)
  if(any(idx)) {
    warning("There are ", sum(idx), " missing times being removed from the data.")
    x <- x[!idx, ]
  }
  if(sort) {
    x <- dplyr::arrange(x, .data[[id]], .data$feature, dplyr::desc(.data$datetime))
  }
  if(time_units == "days" && lubridate::is.POSIXct(x$datetime)) {
    x$datetime <- lubridate::as_date(x$datetime)
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

  if(any(x$exposure_start >= x$exposure_stop)) {
    stop("There are zero- or negative-length exposures.")
  }


  if(check_overlap) {
    y <- x
    y$.exposure.row <- seq_len(nrow(y))
    y$..tv.id.. <- y[[id]]
    z <- dplyr::inner_join(
      y, y, relationship = "many-to-many",
      by = dplyr::join_by(
        x$..tv.id.. == y$..tv.id..,
        x$.exposure.row < y$.exposure.row,
        y$exposure_start < x$exposure_stop,
        y$exposure_stop > x$exposure_start
      )
    )
    z$..tv.id.. <- NULL
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
    event = "event",
    error = "error"
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
