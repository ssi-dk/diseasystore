#' Interlace the secondary tables by the keys and validity of the primary table
#'
#' @description
#'   Using the primary table as the basis of keys and validity, the secondary tables are joined to the
#'   primary table according to the shared keys and then truncated to the validity of the primary table
#'
#' @param primary (`.data`)\cr
#'   The table with the keys the interlacing is done by (first "key_" column in primary).
#'   The secondary tables are matched to the validity interval of the primary table before interlacing
#' @param secondary (`list`(`.data`))\cr
#'   A list of tables that should be matched and joined to the primary table
#' @return An object the same type as the primary. If secondary is NULL, primary is returned.
#'   If secondary is given, the output is the join of the secondary onto primary.
#' @examples
#'   # Lets create some synthetic data
#'   data <- dplyr::mutate(mtcars, "key_name" = rownames(mtcars))
#'
#'   x <- dplyr::select(data, "key_name", "mpg", "cyl")
#'   y <- dplyr::select(data, "key_name", "wt", "vs")
#'   z <- dplyr::select(data, "key_name", "qsec")
#'
#'   # In x, the mpg was changed on 2000-01-01 for all but the first 10 cars
#'   x <- list(utils::head(x, 10) |>
#'               dplyr::mutate("valid_from" = as.Date("1990-01-01"),
#'                             "valid_until" = as.Date(NA)),
#'             utils::tail(x, base::nrow(x) - 10) |>
#'               dplyr::mutate("valid_from" = as.Date("1990-01-01"),
#'                             "valid_until" = as.Date("2000-01-01")),
#'             utils::tail(x, base::nrow(x) - 10) |>
#'               dplyr::mutate("mpg" = 0.9 * mpg,
#'                             "valid_from" = as.Date("2000-01-01"),
#'                             "valid_until" = as.Date(NA))) |>
#'     purrr::reduce(dplyr::union_all)

#'
#'   # In y, the wt was changed on 2010-01-01 for all but the last 10 cars
#'   y <- list(utils::head(y, base::nrow(y) - 10) |>
#'               dplyr::mutate("valid_from" = as.Date("1990-01-01"),
#'                             "valid_until" = as.Date(NA)),
#'             utils::tail(y, 10) |>
#'               dplyr::mutate("valid_from" = as.Date("1990-01-01"),
#'                             "valid_until" = as.Date("2010-01-01")),
#'             utils::tail(y, 10) |>
#'               dplyr::mutate(wt = 1.1 * wt,
#'                             "valid_from" = as.Date("2010-01-01"),
#'                             "valid_until" = as.Date(NA))) |>
#'     purrr::reduce(dplyr::union_all)
#'
#'
#'   # In z, the qsec was changed on 2020-01-01 for all but the last and first 10 cars
#'   z <- list(utils::head(z, base::nrow(z) - 10) |>
#'               dplyr::mutate("valid_from" = as.Date("1990-01-01"),
#'                             "valid_until" = as.Date(NA)),
#'             utils::tail(z, 10) |>
#'               dplyr::mutate("valid_from" = as.Date("1990-01-01"),
#'                             "valid_until" = as.Date(NA)),
#'             utils::head(z, base::nrow(z) - 10) |>
#'               utils::tail(base::nrow(z) - 10) |>
#'               dplyr::mutate("valid_from" = as.Date("1990-01-01"),
#'                             "valid_until" = as.Date("2020-01-01")),
#'             utils::head(z, base::nrow(z) - 10) |>
#'               utils::tail(base::nrow(z) - 10) |>
#'               dplyr::mutate(qsec = 1.1 * qsec,
#'                             "valid_from" = as.Date("2020-01-01"),
#'                             "valid_until" = as.Date(NA))) |>
#'     purrr::reduce(dplyr::union_all)
#'
#'
#'   # We choose a primary interval to interlace on
#'   primary <- dplyr::transmute(data, key_name,
#'                               valid_from = as.Date("1985-01-01"),
#'                               valid_until = as.Date(NA))
#'
#'
#'   # Perform the truncated interlace
#'   truncate_interlace(primary, secondary = list(x, y, z))
#'
#' @importFrom rlang .data
#' @noRd
truncate_interlace <- function(primary, secondary = NULL) {

  # Check edge case
  if (is.null(secondary)) return(primary)

  # Ensure secondary is list
  if (!inherits(secondary, "list")) secondary <- list(secondary)

  # Determine the keys of the primary data
  primary_keys <- colnames(primary)[startsWith(colnames(primary), "key_")]

  # Find all secondary information that is valid while primary information is valid
  secondary_truncated <- secondary |>
    purrr::map(~ {
      # First we find keys that are common with current secondary table and the primary table
      common_keys <- intersect(primary_keys, colnames(.x)[startsWith(colnames(.x), "key_")])

      if (length(common_keys) == 0) stop("No common keys found to interlace by!")

      # We then join the tables by these keys and truncate the secondary table to validity range of the primary
      dplyr::left_join(x = primary, y = .x, suffix = c("", ".y"), by = common_keys) |>
        dplyr::filter((.data$valid_from  < .data$valid_until.y) |   # Keep secondary records
                        is.na(.data$valid_until.y),                 # that is within validity
                      (.data$valid_until > .data$valid_from.y)  |   # of the primary data.
                        is.na(.data$valid_until) |
                        is.na(.data$valid_until.y)) |>
        dplyr::mutate(
          "valid_from"  = ifelse(.data$valid_from  >= .data$valid_from.y,  .data$valid_from, .data$valid_from.y),
          "valid_until" = ifelse(.data$valid_until <= .data$valid_until.y, .data$valid_from, .data$valid_from.y)
          ) |>
        dplyr::select(-tidyselect::ends_with(".y"))
    })

  # With the secondary data truncated, we can interlace and return
  out <- SCDB::interlace_sql(secondary_truncated, by = purrr::pluck(primary_keys, 1))

  return(out)
}
