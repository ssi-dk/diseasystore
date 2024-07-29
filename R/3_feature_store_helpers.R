#' Transform case definition to PascalCase
#' @param label `r rd_diseasystore_label()`
#' @return The given label formatted to match a Diseasystore
#' @examples
#'   to_diseasystore_case("Google COVID-19")  # DiseasystoreGoogleCovid19
#' @export
to_diseasystore_case <- function(label) {

  # First convert to diseasystore case
  diseasystore_case <- label |>
    stringr::str_replace_all(stringr::fixed("_"), " ") |>
    stringr::str_replace_all("(?<=[a-z])([A-Z])", " \\1") |>
    stringr::str_to_title() |>
    stringr::str_replace_all(stringr::fixed(" "), "") |>
    stringr::str_replace_all(stringr::fixed("-"), "") |>
    (\(.) paste0("Diseasystore", .))()

  return(diseasystore_case)
}

#' Detect available diseasystores
#' @return The installed diseasystores on the search path
#' @examples
#'   available_diseasystores()  # DiseasystoreGoogleCovid19 + more from other packages
#' @export
available_diseasystores <- function() {

  # Get all installed packages that has the ^diseasystore.* pattern
  available_diseasystores <- purrr::keep(search(), ~ stringr::str_detect(., ":diseasystore\\.*"))

  # Give warning if none is found
  if (length(available_diseasystores) == 0) stop("No diseasystores found. Have you attached the libraries?")

  # Show available feature stores
  available_diseasystores <- available_diseasystores |>
    purrr::map(ls) |>
    purrr::reduce(c) |>
    purrr::keep(~ startsWith(., "Diseasystore")) |>
    purrr::discard(~ . == "DiseasystoreBase")

  return(available_diseasystores)
}


#' Check for the existence of a `diseasystore` for the case definition
#' @param label `r rd_diseasystore_label()`
#' @return TRUE if the given diseasystore can be matched to a diseasystore on the search path. FALSE otherwise.
#' @examples
#'   diseasystore_exists("Google COVID-19")  # TRUE
#'   diseasystore_exists("Non existent diseasystore")  # FALSE
#' @export
diseasystore_exists <- function(label) {

  checkmate::assert_character(label)

  # Convert label to DiseasystorePascalCase and check existence
  return(to_diseasystore_case(label) %in% available_diseasystores())
}


#' Get the `diseasystore` for the case definition
#' @param label `r rd_diseasystore_label()`
#' @return The diseasystore generator for the diseasystore matching the given label
#' @examples
#'   ds <- get_diseasystore("Google COVID-19")  # Returns the DiseasystoreGoogleCovid19 generator
#' @export
get_diseasystore <- function(label) {
  checkmate::assert_true(diseasystore_exists(label))

  return(get(to_diseasystore_case(label)))
}


#' File path helper for source_conn
#'
#' @description
#'   This helper determines whether source_conn is a file path or URL and creates the full path to the
#'   the file as needed based on the type of source_conn
#' @param source_conn (`character(1)`)\cr
#'   File location (path or URL)
#' @param file (`character(1)`)\cr
#'   Name of the file at the location
#' @noRd
source_conn_path <- function(source_conn, file) {
  url_regex <- r"{\b(?:https?|ftp):\/\/[-A-Za-z0-9+&@#\/%?=~_|!:,.;]*[-A-Za-z0-9+&@#\/%=~_|]}"
  checkmate::assert(
    checkmate::check_directory_exists(source_conn),
    checkmate::check_character(source_conn, pattern = url_regex)
  )

  # Determine the type of location
  if (checkmate::test_directory_exists(source_conn)) { # source_conn is a directory
    # If source_conn is a directory, look for files in the folder and keep the ones that match the requested file
    # This way, if the file exists in a zipped form, it is still retrieved
    matching_file <- purrr::keep(dir(source_conn), ~ startsWith(., file)) |>
      purrr::pluck(1) # Ensure we only have one match

    if (is.null(matching_file)) stop(file, " could not be found in ", source_conn)

    file_location <- file.path(source_conn, matching_file)

  } else if (checkmate::test_character(source_conn, pattern = url_regex)) { # source_conn is a URL
    file_location <- file.path(stringr::str_remove(source_conn, "/$"), file)
  } else {
    stop("source_conn could not be parsed to valid directory or URL\n")
  }

  return(file_location)
}
