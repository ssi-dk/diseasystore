# Store the current options (for later conservation test -- see test-zzz.R)
diseasy_opts <- purrr::keep(names(options()), ~ startsWith(., "diseasystore.")) |>
  purrr::map(options) |>
  purrr::reduce(c)

# Store the current files (for later conservation test -- see test-zzz.R)
current_files <- dir(recursive = TRUE)

# Configure diseasystore for testing
target_schema_1 <- "test_ds"
target_schema_2 <- "not_test_ds"


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

  # Check test schemas are empty
  n_tables_in_test_schemas <- SCDB::get_tables(conn) |>
    dplyr::filter(.data$schema %in% c(target_schema_1, target_schema_2)) |>
    nrow()
  checkmate::assert_true(n_tables_in_test_schemas == 0)

  # Disconnect
  DBI::dbDisconnect(conn)
}
