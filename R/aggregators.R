# We have to wrap the summarize here, since across does stupid caller env blocking


#' Feature aggregators
#'
#' @name aggregators
#' @param .data `r rd_.data()`
#' @param feature (`character`)\cr
#'   Name of the feature to perform the aggregation over
#' @return A dplyr::summarize to aggregate the features together using the given function (sum/max/min/count)
#' @examples
#'   # Primarily used within the framework but can be used individually:
#'
#'   data <- dplyr::mutate(mtcars, key_name = rownames(mtcars), .before = dplyr::everything())
#'
#'   key_join_sum(data, "mpg")    # sum(mtcars$mpg)
#'   key_join_max(data, "mpg")    # max(mtcars$mpg)
#'   key_join_min(data, "mpg")    # min(mtcars$mpg)
#'   key_join_count(data, "mpg")  # nrow(mtcars)
#' @export
key_join_sum <- function(.data, feature) {
  return(dplyr::summarize(.data,
                          dplyr::across(.cols = tidyselect::all_of(feature),
                                        .fns = list(n = ~ sum(as.numeric(.), na.rm = TRUE)),
                                        .names = "{.fn}"),
                          .groups = "drop"))
}

#' @rdname aggregators
#' @export
key_join_max <- function(.data, feature) {
  return(dplyr::summarize(.data,
                          dplyr::across(.cols = tidyselect::all_of(feature),
                                        .fns = list(n = ~ max(as.numeric(.), na.rm = TRUE)),
                                        .names = "{.fn}"),
                          .groups = "drop"))
}

#' @rdname aggregators
#' @export
key_join_min <- function(.data, feature) {
  return(dplyr::summarize(.data,
                          dplyr::across(.cols = tidyselect::all_of(feature),
                                        .fns = list(n = ~ min(as.numeric(.), na.rm = TRUE)),
                                        .names = "{.fn}"),
                          .groups = "drop"))
}

#' @rdname aggregators
#' @export
key_join_count <- function(.data, feature) {
  return(dplyr::summarize(.data,
                          dplyr::across(.cols = purrr::pluck(tidyselect::starts_with("key"), 1),
                                        .fns = list(n = ~ dplyr::n()),
                                        .names = "{.fn}"),
                          .groups = "drop"))
}
