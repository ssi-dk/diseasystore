#' Drop feature stores from DB
#' @importFrom rlang .data
#' @param pattern (`character(1)`)\cr
#'   Pattern to match the tables by
#' @param schema (`character(1)`)\cr
#'   Schema the diseasystore uses to store data in
#' @param conn `r rd_conn()`
#' @return `r rd_side_effects`
#' @examples
#'   conn <- SCDB::get_connection(drv = RSQLite::SQLite())
#'
#'   drop_diseasystore(conn = conn)
#'
#'   DBI::dbDisconnect(conn)
#' @export
drop_diseasystore <- function(pattern = NULL,
                              schema = diseasyoption("target_schema"),
                              conn = SCDB::get_connection()) {

  coll <- checkmate::makeAssertCollection()
  checkmate::assert_character(pattern, null.ok = TRUE, add = coll)
  checkmate::assert_character(schema, add = coll)
  checkmate::assert_class(conn, "DBIConnection", add = coll)
  checkmate::reportAssertions(coll)

  # List all tables
  tables <- SCDB::get_tables(conn, pattern)

  # Early return if no tables are found
  if (nrow(tables) == 0) {
    return(NULL)
  }

  # Concatenate schema and table to ids
  tables <- tables |>
    dplyr::mutate("schema" = dplyr::if_else(is.na(.data$schema), SCDB::get_schema(conn), .data$schema)) |>
    tidyr::unite("db_table_id", "schema", "table", sep = ".", na.rm = TRUE, remove = FALSE)


  # Determine the schema-structure of the database via SCDB::id.
  # E.g. if a database does not contain the schema, the main schema is used.
  # Rather than manually handing these cases, we let SCDB resolve the location where the diseasystore tables are stored.
  # SCDB::id will determine the schema / table structure that we need to delete.
  # We construct a regex from this structure to match the tables to delete.
  # E.g. SQLite without schema: # regex = "main.<schema>."
  # E.g. SQLite with schema: # regex = "<schema>."
  regex_id <- SCDB::id(paste(schema, NULL, sep = "."), conn)

  if (packageVersion("SCDB") <= "0.3") {
    regex <- paste(
      c(
        purrr::pluck(regex_id, "name", "schema", .default = SCDB::get_schema(conn)),
        purrr::pluck(regex_id, "name", "table")
      ),
      collapse = "."
    )
  } else {
    regex <- as.character(regex_id)
  }

  # Use regex to match tables to delete
  tables_to_delete <- dplyr::filter(tables, stringr::str_detect(.data$db_table_id, paste0("^", regex, pattern)))

  # Ensure schema is the same for all identified tables (if not, we have unwanted ambiguity)
  if (length(unique(dplyr::pull(tables_to_delete, "schema"))) > 1) {
    stop("Tables marked for deletion spread across schemas. Unwanted ambiguity detected!")
  }

  # Check if the table "logs" is in the list of tables tp delete, if yes, all tables must be deleted.
  if ("logs" %in% tables_to_delete$table &&
        !identical(tables_to_delete,
                   dplyr::filter(tables, stringr::str_detect(.data$db_table_id, paste0("^", regex))))) {
    stop(glue::glue("'{schema}.logs' set to delete. Can only delete if entire feature store is dropped!"))
  }

  tables_to_delete |>
    purrr::walk(~ DBI::dbRemoveTable(conn, SCDB::id(.x, conn = conn)))

  # Delete from logs
  if (SCDB::table_exists(conn, glue::glue("{schema}.logs"))) {
    log_records_to_delete <- SCDB::get_table(conn, glue::glue("{schema}.logs")) |>
      tidyr::unite("db_table_id", "schema", "table", sep = ".", na.rm = TRUE, remove = FALSE) |>
      dplyr::filter(.data$db_table_id %in% tables_to_delete) |>
      dplyr::select("log_file")

    dplyr::rows_delete(dplyr::tbl(conn, SCDB::id(glue::glue("{schema}.logs"), conn), check_from = FALSE),
                       log_records_to_delete,
                       by = "log_file",
                       in_place = TRUE,
                       unmatched = "ignore")
  }
}
