#' Provides sortable labels for age groups
#' @param age_cuts (`numeric()`)\cr
#'   The lower bound of the groups (0 is implicitly included).
#' @return A vector of labels with zero-padded numerics so they can be sorted easily.
#' @examples
#'   age_labels(c(5, 12, 20, 30))
#' @export
age_labels <- function(age_cuts) {
  checkmate::assert_numeric(age_cuts, any.missing = FALSE, lower = 0, unique = TRUE, sorted = TRUE)

  age_cuts <- age_cuts[age_cuts > 0 & is.finite(age_cuts)]
  width <- nchar(as.character(max(c(0, age_cuts))))

  age_labels <- stringr::str_c(
    stringr::str_pad(c(0, age_cuts), width, pad = "0"),
    c(rep("-", length(age_cuts)), "+"),
    c(stringr::str_pad(age_cuts - 1, width, pad = "0"), "")
  )

  return(age_labels)
}


#' Compute the age (in years) on a given date
#'
#' @description
#'   Provides the sql code to compute the age of a person on a given date.
#' @param birth (`character(1)`)\cr
#'   Name of the birth date column.
#' @param reference_date (`Date(1)` or `character(1)`)\cr
#'   The date to compute the age for (or name of column containing the reference date).
#' @param conn `r rd_conn()`
#' @return SQL query that computes the age on the given date.
#' @examplesIf requireNamespace("RSQLite", quietly = TRUE)
#'   conn <- SCDB::get_connection(drv = RSQLite::SQLite())
#'
#'   dplyr::copy_to(conn, data.frame(birth = as.Date("2001-04-03"), "test_age")) |>
#'     dplyr::mutate(age = !!age_on_date("birth", as.Date("2024-02-28"), conn))
#'
#'   DBI::dbDisconnect(conn)
#' @export
age_on_date <- function(birth, reference_date, conn) {
  checkmate::assert_character(birth)
  checkmate::assert(
    checkmate::check_date(reference_date),
    checkmate::check_character(reference_date)
  )
  checkmate::assert_class(conn, "DBIConnection")

  UseMethod("age_on_date", conn)
}

#' @export
age_on_date.SQLiteConnection <- function(birth, reference_date, conn) {
  age_warning <- paste(
    "Age computation on SQLite is not precise. For some edge-cases, age will be off by 1 year!",
    "Consider using DuckDB as a local database with precise age computation."
  )
  pkgcond::pkg_warning(age_warning)

  if (inherits(reference_date, "Date")) reference_date <- as.numeric(reference_date)
  return(dplyr::sql(glue::glue("FLOOR(({reference_date} - {birth})/365.242374)")))
}

#' @export
age_on_date.PqConnection <- function(birth, reference_date, conn) {
  if (inherits(reference_date, "Date")) reference_date <- glue::glue("'{reference_date}'")
  return(dplyr::sql(glue::glue("DATE_PART('YEAR', AGE({reference_date}, {birth}))")))
}

#' @export
`age_on_date.Microsoft SQL Server` <- function(birth, reference_date, conn) {
  if (inherits(reference_date, "Date")) reference_date <- glue::glue("'{reference_date}'")
  return(
    dplyr::sql(
      glue::glue(
        "CASE ",
        "WHEN DATEADD(year, DATEDIFF(year, {birth}, {reference_date}), {birth}) > {reference_date} ",
        "THEN DATEDIFF(year, {birth}, {reference_date}) - 1 ",
        "ELSE DATEDIFF(year, {birth}, {reference_date}) ",
        "END"
      )
    )
  )
}

#' @export
age_on_date.duckdb_connection <- function(birth, reference_date, conn) {
  if (inherits(reference_date, "Date")) reference_date <- glue::glue("DATE '{reference_date}'")
  return(dplyr::sql(glue::glue("DATE_SUB('year', {birth}, {reference_date})")))
}


#' Backend-dependent time interval (in years)
#'
#' @description
#'   Provides the sql code for a time interval (in years).
#' @param reference_date (`Date(1)` or `character(1)`)\cr
#'   The date to add years to (or name of column containing the reference date).
#' @param years (`numeric(1)` or `character(1)`)\cr
#'   The length of the time interval in whole years (or name of column containing the number of years).
#' @param conn `r rd_conn()`
#' @return SQL query for the time interval.
#' @examplesIf requireNamespace("RSQLite", quietly = TRUE)
#'   conn <- SCDB::get_connection(drv = RSQLite::SQLite())
#'
#'   dplyr::copy_to(conn, data.frame(birth = as.Date("2001-04-03"), "test_age")) |>
#'     dplyr::mutate(first_birthday = !!add_years("birth", 1, conn))
#'
#'   DBI::dbDisconnect(conn)
#' @export
add_years <- function(reference_date, years, conn) {
  checkmate::assert(
    checkmate::check_date(reference_date),
    checkmate::check_character(reference_date)
  )
  checkmate::assert(
    checkmate::check_integerish(years),
    checkmate::check_character(years)
  )
  checkmate::assert_class(conn, "DBIConnection")

  UseMethod("add_years", conn)
}

#' @export
add_years.SQLiteConnection <- function(reference_date, years, conn) {
  time_warning <- paste(
    "Time computation on SQLite is not precise! For long time intervals, the result may be off by 1+ days.",
    "Consider using DuckDB as a local database with precise age computation."
  )
  pkgcond::pkg_warning(time_warning)

  if (inherits(reference_date, "Date")) reference_date <- as.numeric(reference_date)
  return(dplyr::sql(glue::glue("ROUND({reference_date} + {years} * 365.242374)")))
}

#' @export
add_years.PqConnection <- function(reference_date, years, conn) {
  if (inherits(reference_date, "Date")) reference_date <- glue::glue("'{reference_date}'::date")
  return(dplyr::sql(glue::glue("({reference_date} + {years} * INTERVAL '1 year')::date")))
}

#' @export
`add_years.Microsoft SQL Server` <- function(reference_date, years, conn) {
  if (inherits(reference_date, "Date")) reference_date <- glue::glue("CAST('{reference_date}' AS DATE)")
  return(dplyr::sql(glue::glue("DATEADD(year, {years}, {reference_date})")))
}

#' @export
add_years.duckdb_connection <- function(reference_date, years, conn) {
  if (inherits(reference_date, "Date")) reference_date <- glue::glue("DATE '{reference_date}'")
  return(
    dplyr::sql(glue::glue("DATE_TRUNC('day', DATE_ADD({reference_date}, CAST({years} AS bigint) * INTERVAL 1 YEAR))"))
  )
}
