#' Time-varying aggregation functions
#'
#' @param value A vector of values
#' @param datetime A datetime
#' @param current_time The current grid row's time
#' @param ... Other arguments (not used at this time)
#' @return A scalar, indicating the corresponding aggregation over \code{value} or \code{datetime}.
#' @name tv_aggregation
NULL

#' @rdname tv_aggregation
#' @export
tv_count <- function(value, ...) {
  length(value)
}

#' @rdname tv_aggregation
#' @export
tv_any <- function(value, ...) {
  +(length(value) > 0)
}


#' @rdname tv_aggregation
#' @export
tv_lvcf <- function(value, datetime, ...) {
  if(length(value) == 0) NA_real_ else value[which.max(datetime)]
}


#' @rdname tv_aggregation
#' @export
tv_ts <- function(datetime, current_time, ...) {
  if(length(datetime) == 0) NA_real_ else current_time - max(datetime)
}

#' @rdname tv_aggregation
#' @export
tv_min <- function(value, ...) {
  if(length(value) == 0 || all(is.na(value))) NA_real_ else min(value, na.rm = TRUE)
}

#' @rdname tv_aggregation
#' @export
tv_max <- function(value, ...) {
  if(length(value) == 0 || all(is.na(value))) NA_real_ else max(value, na.rm = TRUE)
}

#' @rdname tv_aggregation
#' @export
tv_mean <- function(value, ...) {
  if(length(value) == 0 || all(is.na(value))) NA_real_ else mean(value, na.rm = TRUE)
}

#' @rdname tv_aggregation
#' @export
tv_median <- function(value, ...) {
  if(length(value) == 0 || all(is.na(value))) NA_real_ else stats::median(value, na.rm = TRUE)
}

#' @rdname tv_aggregation
#' @export
tv_sum <- function(value, ...) {
  sum(value, na.rm = TRUE)
}
