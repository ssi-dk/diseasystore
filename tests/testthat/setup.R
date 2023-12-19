# Store the current options (for later conservation test -- see test-zzz.R)
diseasy_opts <- purrr::keep(names(options()), ~ startsWith(., "diseasystore.")) |>
  purrr::map(options) |>
  purrr::reduce(c)

# Store the current files (for later conservation test -- see test-zzz.R)
current_files <- dir(recursive = TRUE)

# Configure diseasystore for testing
target_schema_1 <- "test_ds"
target_schema_2 <- "not_test_ds"

#' Get a list of data base connections to test on
#' @return
#'   If you run your tests locally, it returns a list of connections corresponding to conn_list and conn_args
#'   If you run your tests on GitHub, it return a list of connection corresponding to the environment variables.
#'   i.e. the GitHub workflows will configure the testing back ends
#' @importFrom rlang `:=`
#' @noRd
get_test_conns <- function() {

  # Locally use rlang's (without this, it may not be bound)
  `:=` <- rlang::`:=`

  # Check if we run remotely
  running_locally <- !identical(Sys.getenv("CI"), "true")

  # Define list of connections to check
  if (running_locally) {

    # Define our local connection backends
    conn_list <- list(
      # Backend string = package::function
      "SQLite"     = "RSQLite::SQLite",
      "PostgreSQL" = "RPostgres::Postgres"
    )

  } else {
    # Use the connection configured by the remote
    conn_list <- tibble::lst(
      !!Sys.getenv("BACKEND", unset = "SQLite") := !!Sys.getenv("BACKEND_DRV", unset = "RSQLite::SQLite")               # nolint: object_name_linter
    )
  }

  # Define list of args to conns
  if (running_locally) {

    # Define our local connection arguments
    conn_args <- list(
      # Backend string = list(named args)
      "SQLite" = list(dbname = tempfile())
    )

  } else {
    # Use the connection configured by the remote
    conn_args <- tibble::lst(
      !!Sys.getenv("BACKEND", unset = "SQLite") :=                                                                      # nolint: object_name_linter
        !!Sys.getenv("BACKEND_ARGS", unset = "list(dbname = tempfile())")                                               # nolint: object_name_linter
    ) |>
      purrr::map(~ eval(parse(text = .)))
  }


  get_driver <- function(x = character(), ...) {                                                                        # nolint: object_usage_linter
    if (!grepl(".*::.*", x)) stop("Package must be specified with namespace (e.g. RSQLite::SQLite)!\n",
                                  "Received: ", x)
    parts <- strsplit(x, "::", fixed = TRUE)[[1]]

    # Skip unavailable packages
    if (!requireNamespace(parts[1], quietly = TRUE)) {
      return()
    }

    drv <- getExportedValue(parts[1], parts[2])

    conn <- tryCatch(
      SCDB::get_connection(drv = drv(), ...),
      error = function(e) {
        return(NULL) # Return NULL, if we cannot connect
      }
    )


    # SQLite back end gives an error in SCDB if there are no tables (it assumes a bad configuration)
    # We create a table to suppress this warning
    if (checkmate::test_class(conn, "SQLiteConnection")) {
      DBI::dbWriteTable(conn, "iris", iris)
    }

    return(conn)
  }

  # Create connection generator
  conn_configuration <- dplyr::left_join(
    tibble::tibble(backend = names(conn_list), conn_list = unname(unlist(conn_list))),
    tibble::tibble(backend = names(conn_args), conn_args),
    by = "backend"
  )

  test_conns <- purrr::pmap(conn_configuration, ~ purrr::partial(get_driver, x = !!..2, !!!..3)())
  names(test_conns) <- conn_configuration$backend
  test_conns <- purrr::discard(test_conns, is.null)

  return(test_conns)
}


# Ensure the target conns are empty and configured correctly
for (conn in get_test_conns()) {

  # Try to write to the target schema
  test_id <- SCDB::id(paste(target_schema_1, "mtcars", sep = "."), conn)

  # Delete existing
  if (DBI::dbExistsTable(conn, test_id)) {
    DBI::dbRemoveTable(conn, test_id)
  }

  # Check write permissions
  DBI::dbWriteTable(conn, test_id, mtcars)
  if (!DBI::dbExistsTable(conn, test_id)) {
    rlang::abort("Cannot write to test schema (", target_schema_1, "). Check DB permissions.")
  }

  # Delete the existing data in the schema
  drop_diseasystore(schema = target_schema_1, conn = conn)
  drop_diseasystore(schema = target_schema_2, conn = conn)

  # Disconnect
  DBI::dbDisconnect(conn)
}


# Report testing environment to the user
message(
  "#####\n\n",
  "Following drivers will be tested:",
  sep = ""
)

conns <- get_test_conns()
message(sprintf("  %s\n", names(conns)))
message("#####")

# Disconnect
purrr::walk(conns, ~ DBI::dbDisconnect)
