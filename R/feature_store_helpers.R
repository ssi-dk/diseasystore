#' Transform case definition to PascalCase
#' @template case_definition
#' @export
diseasystore_case_definition <- function(case_definition) {

  case_definition |>
    stringr::str_replace_all("_", " ") |>
    stringr::str_replace_all("(?<=[a-z])([A-Z])", " \\1") |>
    stringr::str_to_title() |>
    stringr::str_replace_all(" ", "") |>
    stringr::str_replace_all("-", "") |>
    (\(.) paste0("Diseasystore", .))()

}


#' Detect available diseasystores
#' @export
available_diseasystores <- function() {
  # Get all installed packages that has the ^diseasystore.* pattern
  available_diseasystores <- purrr::keep(search(), ~ stringr::str_detect(., ":diseasystore\\.*"))

  # Give warning if none is found
  if (length(available_diseasystores) == 0) stop("No diseasystores found. Have you attached the libraries?")

  # Show available feature stores
  available_diseasystores |>
    purrr::map(ls) |>
    purrr::reduce(c) |>
    purrr::keep(~ startsWith(., "Diseasystore")) |>
    purrr::discard(~ . %in% c("DiseasystoreBase", "DiseasystoreGeneric"))
}


#' Check for the existence of a `diseasystore` for the case definition
#' @template case_definition
#' @export
diseasystore_exists <- function(case_definition) {

  checkmate::assert_character(case_definition)

  # Convert case_definition to DiseasystorePascalCase and check existence
  return(diseasystore_case_definition(case_definition) %in% available_diseasystores())

}


#' Get the `diseasystore` for the case definition
#' @template case_definition
#' @export
get_diseasystore <- function(case_definition) {

  checkmate::assert_true(diseasystore_exists(case_definition))

  return(get(diseasystore_case_definition(case_definition)))
}


#' Existence aware pick operator
#' @param env (`object`)\cr
#'   The object or environment to attempt to pick from
#' @param field (`character`)\cr
#'   The name of the field to pick from `env`
#' @return
#'   Error if the `field` does not exist in `env`, otherwise it returns `field`
#' @export
`%.%` <- function(env, field) {
  field_name <- as.character(substitute(field))
  env_name <- as.character(substitute(env))
  if (is.environment(env)) env <- as.list(env, all.names = TRUE)
  if (!(field_name %in% names(env))) {
    stop(field_name, " not found in ", env_name)
  }
  return(purrr::pluck(env, field_name))
}
