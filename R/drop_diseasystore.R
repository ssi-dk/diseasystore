#' Drop feature stores from DB
#' @importFrom rlang .data
#' @param pattern (`character(1)`)\cr
#'   Pattern to match the tables by
#' @param schema (`character(1)`)\cr
#'   Schema the diseasystore uses to store data in
#' @param conn `r rd_conn()`
#' @return `r rd_side_effects`
#' @examplesIf requireNamespace("RSQLite", quietly = TRUE)
#'   conn <- SCDB::get_connection(drv = RSQLite::SQLite())
#'
#'   drop_diseasystore(conn = conn)
#'
#'   DBI::dbDisconnect(conn)
#' @export
drop_diseasystore <- function(
  pattern = NULL,
  schema = diseasyoption("target_schema", namespace = "diseasystore"),
  conn = SCDB::get_connection()
) {

  coll <- checkmate::makeAssertCollection()
  checkmate::assert_character(pattern, null.ok = TRUE, add = coll)
  checkmate::assert_character(schema, add = coll)
  checkmate::assert_class(conn, "DBIConnection", add = coll)
  checkmate::reportAssertions(coll)


  # List all tables
  tables <- SCDB::get_tables(conn, pattern) |>
    tidyr::unite("db_table_id", tidyselect::any_of(c("catalog", "schema", "table")), sep = ".",
                 na.rm = TRUE, remove = FALSE)

  # Early return if no tables are found
  if (nrow(tables) == 0) {
    return(NULL)
  }

  # Determine the schema-structure of the database via SCDB::id.
  # E.g. if a database does not contain the schema, the main schema is used.
  # Rather than manually handing these cases, we let SCDB resolve the location of the diseasystore tables.
  # SCDB::id will determine the schema / table structure that we need to delete.
  # We construct a regex from this structure to match the tables to delete.
  # E.g. SQLite without schema: # regex = "main.<schema>.<pattern>"
  # E.g. SQLite with schema: # regex = "<schema>.<pattern>"
  ds_table_pattern <- SCDB::id(paste(c(schema, purrr::pluck(pattern, .default = "*")), collapse = "."), conn) |>
    as.character()

  # Get the context (ie. generalised location) of the tables:
  # E.g. SQLite without schema: # regex = "main.<schema>"
  # E.g. SQLite with schema: # regex = "<schema>"
  ds_context <- stringr::str_remove(ds_table_pattern, r"{\.[^\.]+$}")

  # Use regex to match tables to delete
  tables_to_delete <- tables |>
    dplyr::filter(stringr::str_starts(.data$db_table_id, paste0("^", ds_table_pattern))) |>
    dplyr::select(!"db_table_id")

  # Ensure schema is the same for all identified tables (if not, we have unwanted ambiguity)
  if (length(unique(dplyr::pull(tables_to_delete, "schema"))) > 1) {
    stop("Tables marked for deletion spread across schemas. Unwanted ambiguity detected!")
  }

  # Check if the table "logs" is in the list of tables to delete, if yes, all tables must be deleted.
  if (purrr::some(tables_to_delete$table, ~ endsWith(., ".logs")) &&
        !identical(tables_to_delete, SCDB::get_tables(conn, paste0("^", ds_context)))) {
    stop(glue::glue("'{schema}.logs' set to delete. Can only delete if entire feature store is dropped!"))
  }

  # Delete tables
  if (nrow(tables_to_delete) > 0) {
    tables_to_delete |>
      dplyr::group_by(dplyr::row_number()) |>
      dplyr::group_map(SCDB::id) |>
      purrr::walk(~ DBI::dbRemoveTable(conn, .))

    # Delete entries from logs
    logs_table_id <- SCDB::id(paste(c(schema, "logs"), collapse = "."), conn)

    if (SCDB::table_exists(conn, logs_table_id)) {

      log_records_to_delete <- SCDB::get_table(conn, logs_table_id) |>
        SCDB::filter_keys(tables_to_delete, by = colnames(tables_to_delete), copy = TRUE)

      dplyr::rows_delete(
        dplyr::tbl(conn, logs_table_id),
        log_records_to_delete,
        by = colnames(log_records_to_delete),
        in_place = TRUE,
        unmatched = "ignore"
      )
    }
  }
}
