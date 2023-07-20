# We have to wrap the summarize here, since across does stupid caller env blocking


#' Feature aggregators
#'
#' @name aggregators
#' @template .data
#' @param feature (`character`)\cr
#'   Name of the feature to perform the aggregation over
#' @export
key_join_sum <- function(.data, feature) {
  return(dplyr::summarize(.data,
          dplyr::across(.cols = tidyselect::all_of(feature),
                        .fns = list(n = ~ sum(., na.rm = TRUE)),
                        .names = "{.fn}"),
          .groups = "drop"))
}

#' @rdname aggregators
#' @export
key_join_max <- function(.data, feature) {
  return(dplyr::summarize(.data,
           dplyr::across(.cols = tidyselect::all_of(feature),
                         .fns = list(n = ~ max(., na.rm = TRUE)),
                         .names = "{.fn}"),
           .groups = "drop"))
}

#' @rdname aggregators
#' @export
key_join_min <- function(.data, feature) {
  return(dplyr::summarize(.data,
                          dplyr::across(.cols = tidyselect::all_of(feature),
                                        .fns = list(n = ~ min(., na.rm = TRUE)),
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
