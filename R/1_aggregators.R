# We have to wrap the summarise here, since across does stupid caller env blocking


#' @title title
#' Feature aggregators
#'
#' @description
#' When designing a `diseasystore` (see `vignette("extending-diseasystore")`), each feature must specify
#' how that data should be summarised when joined with other data.
#'
#' The automatic coupling and aggregation is essentially a three part process:
#' First, data for the observable is joined with data required to form the stratifications.
#' Next, the combined data is grouped at the level of stratification requested.
#' Finally, the data is summarised in each group to form a single value for the group.
#'
#' Below, we provide a set of simple function that perform this final summarisation step.
#'
#' For semi-aggregated data, a typical key_join aggregator is the `key_join_sum()` which just adds the observations
#' across the group
#'
#' In some cases, such as if the observable is the "maximum temperature" the relevant aggregator would be the
#' `key_join_max()` aggregator which takes the max across the group.
#'
#' When working with individual level data, the observables often consists of a record per individual and to
#' summarise the data we instead need the `key_join_count()` aggregator to get the size of the group.
#'
#' @name aggregators
#' @param .data `r rd_.data()`
#' @param feature (`character`)\cr
#'   Name of the feature to perform the aggregation over
#' @return A dplyr::summarise to aggregate the features together using the given function (sum/max/min/count)
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
  return(
    dplyr::summarise(
      .data,
      dplyr::across(
        .cols = tidyselect::all_of(feature),
        .fns = list(value = ~ sum(as.numeric(.), na.rm = TRUE)),
        .names = "{.fn}"
      ),
      .groups = "drop"
    )
  )
}

#' @rdname aggregators
#' @export
key_join_max <- function(.data, feature) {
  return(
    dplyr::summarise(
      .data,
      dplyr::across(
        .cols = tidyselect::all_of(feature),
        .fns = list(value = ~ max(as.numeric(.), na.rm = TRUE)),
        .names = "{.fn}"
      ),
      .groups = "drop"
    )
  )
}

#' @rdname aggregators
#' @export
key_join_min <- function(.data, feature) {
  return(
    dplyr::summarise(
      .data,
      dplyr::across(
        .cols = tidyselect::all_of(feature),
        .fns = list(value = ~ min(as.numeric(.), na.rm = TRUE)),
        .names = "{.fn}"
      ),
      .groups = "drop"
    )
  )
}

#' @rdname aggregators
#' @export
key_join_count <- function(.data, feature) {
  return(
    dplyr::summarise(
      .data,
      dplyr::across(
        .cols = purrr::pluck(tidyselect::starts_with("key"), 1),
        .fns = list(value = ~ dplyr::n()),
        .names = "{.fn}"
      ),
      .groups = "drop"
    )
  )
}
