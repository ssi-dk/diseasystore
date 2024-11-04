#' File path helper for different source_conn
#'
#' @name source_conn_helpers
#' @description
#'  * source_conn_path: static url / directory.
#'    This helper determines whether source_conn is a file path or URL and creates the full path to the
#'    the file as needed based on the type of source_conn.
#' @param source_conn (`character(1)`)\cr                                                                               # nolint: documentation_template_linter
#'   File location (path or URL).
#' @param file (`character(1)`)\cr
#'   Name (including path) of the file at the location.
#' @return (`character(1)`)\cr
#'   The full path to the requested file.
#' @examples
#'   # Simulating a data directory
#'   source_conn <- "data_dir"
#'   dir.create(source_conn)
#'   write.csv(mtcars, file.path(source_conn, "mtcars.csv"))
#'   write.csv(iris, file.path(source_conn, "iris.csv"))
#'
#'   # Get file path for mtcars.csv
#'   source_conn_path(source_conn, "mtcars.csv")
#'
#'   # Clean up
#'   unlink(source_conn, recursive = TRUE)
#' @export
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


#' @rdname source_conn_helpers
#' @description
#' * source_conn_github: static GitHub API url / git directory.
#'   This helper determines whether source_conn is a git directory or a GitHub API creates the full path to the
#'   the file as needed based on the type of source_conn.
#'
#'   A GitHub token can be configured in the "GITHUB_PAT" environment variable to avoid rate limiting.
#'
#'   If the basename of the requested file contains a date, the function will use fuzzy-matching to determine the
#'   closest matching, chronologically earlier, file location to return.
#' @param pull (`logical(1)`)\cr
#'   Should "git pull" be called on the local repository before reading files?
#' @export
source_conn_github <- function(source_conn, file, pull = TRUE) {
  url_regex <- r"{https?:\/\/api.github.com\/repos\/[a-zA-Z-]*\/[a-zA-Z-]*}"
  checkmate::assert(
    checkmate::check_directory_exists(source_conn),
    checkmate::check_character(source_conn, pattern = url_regex)
  )

  relative_path <- dirname(file)

  # We prepare the incoming file for fuzzy matching
  date_regex <- "[0-9]{4}-[0-9]{2}-[0-9]{2}"
  file_date <- lubridate::ymd(stringr::str_extract(file, date_regex))
  file_pattern <- stringr::str_replace(basename(file), date_regex, date_regex)

  # And generate a small helper function to do the matching
  fuzzy_match <- \(files) {
    data.frame(path = files) |>
      dplyr::filter(stringr::str_detect(.data$path, file_pattern)) |>
      dplyr::mutate("file_date" = as.Date(stringr::str_extract(.data$path, date_regex))) |>
      dplyr::filter(.data$file_date <= !!file_date) |>
      dplyr::slice_max(.data$file_date) |>
      dplyr::pull("path")
  }


  # Determine the type of location
  if (checkmate::test_directory_exists(source_conn)) { # source_conn is a directory
    # If source_conn is a directory, ensure repos is updated, then look for files in the folder and keep the ones that
    # match the requested file.

    # Update the local repo -- give warning if we cannot
    if (pull) {
      if (!checkmate::test_directory_exists(file.path(source_conn, ".git"))) {
        stop("The directory ", source_conn, " does not appear to be a git repository. Cannot pull.")
      }
      tryCatch(
        msg <- system2("git", args = c(paste("-C", source_conn), "pull"), stdout = TRUE),                               # nolint: implicit_assignment_linter
        warning = function(w) {
          stop(paste(c("Your local repository could not be updated!", msg), collapse = "\n"))
        }
      )
    }

    dir_content <- file.path(source_conn, relative_path) |>
      dir(pattern = file_pattern)

    # Perform the fuzzy matching and determine file location
    return_file <- fuzzy_match(dir_content)

    return(file.path(source_conn, relative_path, return_file))


  } else if (checkmate::test_character(source_conn, pattern = url_regex)) { # source_conn is a URL

    # Get repo content from github API at one level higher than the requested path
    # This allows us to determine the sha of the requested path and retrieve the contents
    # of the path using the trees API instead of the content API.
    # The former API has a limit of 100.000 returns whereas the latter has limit of 1000 returns.
    # Since the contents can contain more than 1000 files within a few years, we go the extra step
    # now to use the trees API

    handle <- curl::new_handle()
    token <- Sys.getenv("GITHUB_PAT")

    if (!is.null(token)) {
      curl::handle_setheaders(
        handle,
        "Content-Type" = "application/json",
        "Authorization" = glue::glue("Bearer {token}")
      )
    }

    repo_content <- curl::curl(glue::glue("{source_conn}/contents/{dirname(relative_path)}"), handle = handle) |>
      jsonlite::fromJSON()

    # Determine sha of the relative path
    dir_sha <- repo_content |>
      dplyr::filter(.data$name == basename(relative_path)) |>
      dplyr::pull("sha")


    # Get all files at the relative path
    dir_content <- curl::curl(glue::glue("{source_conn}/git/trees/{dir_sha}"), handle = handle) |>
      jsonlite::fromJSON()

    if (dir_content$truncated) {
      warning(
        "Data returned from GitHub API has been truncated! ",
        "The returned file may not be correct.",
        "Consider cloning the repository and use a local source_conn."
      )
    }

    # Perform the fuzzy matching and generate a download link
    return_file <- fuzzy_match(dir_content$tree$path)

    file_path <- curl::curl(glue::glue("{source_conn}/contents/{relative_path}/{return_file}"), handle = handle) |>
      jsonlite::fromJSON() |>
      purrr::pluck("download_url")

    return(file_path)

  } else {
    stop("source_conn could not be parsed to valid GitHub repository or GitHub API URL\n")
  }

}
