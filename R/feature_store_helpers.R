#' Transform case definition to PascalCase
#' @param case_definition `r rd_case_definition()`
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
#' @param case_definition `r rd_case_definition()`
#' @export
diseasystore_exists <- function(case_definition) {

  checkmate::assert_character(case_definition)

  # Convert case_definition to DiseasystorePascalCase and check existence
  return(diseasystore_case_definition(case_definition) %in% available_diseasystores())

}


#' Get the `diseasystore` for the case definition
#' @param case_definition `r rd_case_definition()`
#' @export
get_diseasystore <- function(case_definition) {

  checkmate::assert_true(diseasystore_exists(case_definition))

  return(get(diseasystore_case_definition(case_definition)))
}


#' Provides age_labels that follows the mg standard
#' @param age_cuts The lower bound of the groups (0 is implicitly included)
#' @return A vector of labels with zero-padded numerics so they can be sorted easily
#' @examples
#'   age_labels(c(5, 12, 20, 30))
#' @export
age_labels <- function(age_cuts) {
  checkmate::assert_numeric(age_cuts, any.missing = FALSE, lower = 0, unique = TRUE, sorted = TRUE)

  age_cuts <- age_cuts[age_cuts > 0 & is.finite(age_cuts)]
  width <- nchar(as.character(max(c(0, age_cuts))))
  stringr::str_c(stringr::str_pad(c(0, age_cuts), width, pad = "0"),
                 c(rep("-", length(age_cuts)), "+"),
                 c(stringr::str_pad(age_cuts - 1, width, pad = "0"), ""))
}
