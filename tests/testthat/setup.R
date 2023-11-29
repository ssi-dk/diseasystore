# Store the current options
diseasy_opts <- options(purrr::keep(names(options()), ~ startsWith(., "diseasy")))

# Configure diseasystore for testing
target_schema_1 <- "test_ds"
target_schema_2 <- "not_test_ds"

# Define list of connections to check
conn_list <- list(
  # Backend string = package::function
  "SQLite"     = "RSQLite::SQLite",
  "PostgreSQL" = "RPostgres::Postgres"
)

# Define list of args to conns
conn_args <- list(
  # Backend string = list(named args)
  "SQLite" = list(dbname = tempfile())
)

get_driver <- function(x = character(), ...) {
  if (!grepl(".*::.*", x)) stop("Package must be specified with namespace (e.g. RSQLite::SQLite)!\n",
                                "Received: ", x)
  parts <- strsplit(x, "::")[[1]]

  # Skip unavailable packages
  if (!requireNamespace(parts[1], quietly = TRUE)) {
    return()
  }

  drv <- getExportedValue(parts[1], parts[2])

  tryCatch(suppressWarnings(SCDB::get_connection(drv = drv(), ...)),  # We expect a warning if no tables are found
           error = function(e) {
             NULL # Return NULL, if we cannot connect
           })
}

# Create connection generator
conn_configuration <- dplyr::left_join(
  tibble::tibble(backend = names(conn_list), conn_list = unname(unlist(conn_list))),
  tibble::tibble(backend = names(conn_args), conn_args),
  by = "backend"
)

# Send configuration to get_driver when getting test cons
get_test_conns <- function() {
  test_conns <- purrr::pmap(conn_configuration, ~ purrr::partial(get_driver, x = !!..2, !!!..3)())
  names(test_conns) <- conn_configuration$backend
  test_conns <- purrr::discard(test_conns, is.null)
  return(test_conns)
}
conns <- get_test_conns()

# Ensure the target conns are empty and configured correctly
for (conn in conns) {

  # SQLite back-ends gives an error in SCDB if there are no tables (it assumes a bad configuration)
  # We create a table to suppress this warning
  tryCatch(
    SCDB::get_tables(conn),
    warning = function(w) {
      checkmate::assert_character(w$message, pattern = "No tables found")
      DBI::dbWriteTable(conn, "mtcars", mtcars)
    }
  )

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

}



# Report testing environment to the user
if (length(conns[names(conns) != "SQLite"]) == 0) {
  message("No useful drivers (other than SQLite) were found!")
}

message("#####\n",
        "Following drivers will be tested:\n",
        sprintf("  %s (%s)\n", conn_list[names(conns)], names(conns)),
        sep = "")

unavailable_drv <- conn_list[which(!names(conn_list) %in% names(conns))]
if (length(unavailable_drv) > 0) {
  message("\nFollowing drivers were not found and will NOT be tested:\n",
          sprintf("  %s (%s)\n", conn_list[names(unavailable_drv)], names(unavailable_drv)),
          sep = "")
}
message("#####")


# Close conns
purrr::walk(conns, ~ {
  DBI::dbDisconnect(.)
  rm(.)
})
