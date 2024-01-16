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

  # Get tables to delete
  tables <- SCDB::get_tables(conn, pattern) |>
    tidyr::unite("db_table_id", "schema", "table", sep = ".", na.rm = TRUE) |>
    dplyr::pull("db_table_id")

  tables_to_delete <- tables |>
    purrr::keep(~ stringr::str_detect(., glue::glue("^{schema}\\.{ifelse(is.null(pattern), '', pattern)}")))

  # Check if logs is in the table, if yes, all tables must be deleted
  if ("logs" %in% tables_to_delete &&
        !identical(tables_to_delete, purrr::keep(tables, ~ stringr::str_detect(., glue::glue("^{schema}\\..*"))))) {
    stop(glue::glue("'{schema}.logs' set to delete. Can only delete if entire feature store is dropped."))
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
