#' Time-varying aggregation functions
#'
#' @param x A data.frame with four columns: <id>, "feature", "datetime", "value"
#' @param current_time The current grid row's time
#' @param ... Other arguments (not used at this time)
#' @return A scalar, indicating the corresponding aggregation over \code{x}.
#' @name tv_aggregation
NULL

#' @rdname tv_aggregation
#' @export
tv_count <- function(x, ...) {
  nrow(x)
}

#' @rdname tv_aggregation
#' @export
tv_any <- function(x, ...) {
  +(nrow(x) > 0)
}


#' @rdname tv_aggregation
#' @export
tv_lvcf <- function(x, ...) {
  if(nrow(x) == 0) NA_real_ else x$value[which.max(x$datetime)]
}


#' @rdname tv_aggregation
#' @export
tv_ts <- function(x, current_time, ...) {
  if(nrow(x) == 0) NA_real_ else current_time - max(x$datetime)
}

#' @rdname tv_aggregation
#' @export
tv_min <- function(x, ...) {
  if(nrow(x) == 0 || all(is.na(x$value))) NA_real_ else min(x$value, na.rm = TRUE)
}

#' @rdname tv_aggregation
#' @export
tv_max <- function(x, ...) {
  if(nrow(x) == 0 || all(is.na(x$value))) NA_real_ else max(x$value, na.rm = TRUE)
}

#' @rdname tv_aggregation
#' @export
tv_mean <- function(x, ...) {
  if(nrow(x) == 0 || all(is.na(x$value))) NA_real_ else mean(x$value, na.rm = TRUE)
}

#' @rdname tv_aggregation
#' @export
tv_median <- function(x, ...) {
  if(nrow(x) == 0 || all(is.na(x$value))) NA_real_ else stats::median(x$value, na.rm = TRUE)
}

#' @rdname tv_aggregation
#' @export
tv_sum <- function(x, ...) {
  sum(x$value, na.rm = TRUE)
}
