#' Interlace the secondary tables by the keys and validity of the primary table
#'
#' @description
#' A short description...
#'
#' @param primary
#'   The table with the keys the interlacing is done by (first "key_" column in primary).
#'   The secondary tables are matched to the validity interval of the primary table before interlacing
#' @param secondary A list of tables that should be matched and joined to the primary table
#' @importFrom rlang .data
truncate_interlace <- function(primary, secondary = NULL) {

  # Check edge case
  if (is.null(secondary)) return(primary)

  # Ensure secondary is list
  if (!inherits(secondary, "list")) secondary <- list(secondary)

  # Determine the keys of the primary data
  primary_keys <- colnames(primary)[startsWith(colnames(primary), "key_")]

  # Find all secondary information that is vaild while primary information is valid
  secondary_truncated <- secondary |>
    purrr::map(~ {
      # First we find keys that are common with current secondary table and the primary table
      common_keys <- intersect(primary_keys, colnames(.x)[startsWith(colnames(.x), "key_")])

      if (length(common_keys) == 0) stop("No common keys found to interlace by!")

      # We then join the tables by these keys and truncate the secondary table to validity range of the primary
      dplyr::right_join(x = .x, y = primary, suffix = c(".x", ""), by = common_keys) |>
        dplyr::filter((.data$valid_from  < .data$valid_until.x) |   # Keep secondary records
                        is.na(.data$valid_until.x),                 # that is within validity
                      (.data$valid_until > .data$valid_from.x)  |   # of the primary data.
                        is.na(.data$valid_until) |
                        is.na(.data$valid_until.x)) |>
        dplyr::mutate("valid_from"  = pmax(.data$valid_from,  .data$valid_from.x,  na.rm = TRUE),
                      "valid_until" = pmin(.data$valid_until, .data$valid_until.x, na.rm = TRUE)) |>
        dplyr::select(-tidyselect::ends_with(".x"))
    })

  # With the secondary data truncated, we can interlace and return
  out <- mg_interlace_sql(secondary_truncated, by = purrr::pluck(primary_keys, 1))

  return(out)
}
