#' Transform case definition to PascalCase
#' @template case_definition
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
  ls("package:diseasystore") |>
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


# Existence operator
`%.%` <- function(env, field) {
  field_name <- as.character(substitute(field))
  env_name <- as.character(substitute(env))
  if (is.environment(env)) env <- as.list(env)
  if (!(field_name %in% names(env))) {
    stop(field_name, " not found in ", env_name)
  }
  return(purrr::pluck(env, field_name))
}
