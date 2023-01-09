
#' @rdname tv
#' @export
tv_count <- function(x, ...) {
  nrow(x)
}

#' @rdname tv
#' @export
tv_any <- function(x, ...) {
  +(nrow(x) > 0)
}


#' @rdname tv
#' @export
tv_lvcf <- function(x, ...) {
  if(nrow(x) > 0) x$value[which.max(x$datetime)] else NA_real_
}


#' @rdname tv
#' @export
tv_ts <- function(x, current_time, ...) {
  if(nrow(x) > 0) current_time - max(x$datetime) else NA_real_
}

#' @rdname tv
#' @export
tv_min <- function(x, ...) {
  if(nrow(x) > 0) min(x$value, na.rm = TRUE) else NA_real_
}

#' @rdname tv
#' @export
tv_max <- function(x, ...) {
  if(nrow(x) > 0) max(x$value, na.rm = TRUE) else NA_real_
}

#' @rdname tv
#' @export
tv_mean <- function(x, ...) {
  if(nrow(x) > 0) mean(x$value, na.rm = TRUE) else NA_real_
}

#' @rdname tv
#' @export
tv_sum <- function(x, ...) {
  sum(x$value, na.rm = TRUE)
}
