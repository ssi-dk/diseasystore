# Configure diseasystore for testing
options("diseasystore.target_schema" = "test_ds")

# Define list of connections to check
conn_list <- list(
  # Backend string = package::function
  "SQLite"     = "RSQLite::SQLite",
  "PostgreSQL" = "RPostgres::Postgres"
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
get_test_conns <- \() unlist(lapply(conn_list, get_driver))
conns <- get_test_conns()

# Ensure the target conns are empty and configured correctly
for (conn in conns) {

  # Try to write to the target schema
  test_id <- SCDB::id(paste(target_schema, "mtcars", sep = "."), conn)

  # Delete existing
  if (DBI::dbExistsTable(conn, test_id)) {
    DBI::dbRemoveTable(conn, test_id)
  }

  # Check write permissions
  DBI::dbWriteTable(conn, test_id, mtcars)
  if (!DBI::dbExistsTable(conn, test_id)) {
    stop("Cannot write to test schema (", target_schema, "). Check DB permissions.")
  }

  # Delete the existing data in the schema
  drop_diseasystore(schema = target_schema, conn = conn)
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
