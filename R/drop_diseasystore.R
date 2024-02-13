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


  if (packageVersion("SCDB") < "0.4.0") {
    # Get list of tables
    tables <- SCDB::get_tables(conn, pattern) |>
      tidyr::unite("db_table_id", "schema", "table", sep = ".", na.rm = TRUE) |>
      dplyr::pull("db_table_id")

    # Determine pattern to delete tables with
    ds_table_pattern <- SCDB::id(paste(c(schema, purrr::pluck(pattern, .default = "*")), collapse = "."), conn)
    ds_table_pattern <- paste(
      c(purrr::pluck(ds_table_pattern, "name", "schema"),
        purrr::pluck(ds_table_pattern, "name", "table")
      ),
      collapse = "."
    )
    ds_context <- stringr::str_remove(ds_table_pattern, r"{\.[^\.]+$}")

    tables_to_delete <- purrr::keep(tables, ~ stringr::str_detect(., ds_table_pattern))

    # Check if logs is in the table, if yes, all tables must be deleted
    if ("logs" %in% tables_to_delete &&
          !identical(tables_to_delete, purrr::keep(tables, ~ stringr::str_detect(., paste0(ds_context, ".*"))))) {
      stop(glue::glue("'{schema}.logs' set to delete. Can only delete if entire feature store is dropped."))
    }

    tables_to_delete |>
      purrr::walk(~ DBI::dbRemoveTable(conn, SCDB::id(.x, conn = conn)))

    # Delete from logs
    if (SCDB::table_exists(conn, glue::glue("{ds_context}.logs"))) {
      log_records_to_delete <- SCDB::get_table(conn, glue::glue("{ds_context}.logs")) |>
        tidyr::unite("db_table_id", "schema", "table", sep = ".", na.rm = TRUE, remove = FALSE) |>
        dplyr::filter(.data$db_table_id %in% tables_to_delete) |>
        dplyr::select("log_file")

      dplyr::rows_delete(dplyr::tbl(conn, SCDB::id(glue::glue("{ds_context}.logs"), conn), check_from = FALSE),
                         log_records_to_delete,
                         by = "log_file",
                         in_place = TRUE,
                         unmatched = "ignore")
    }

  } else {
    # Get list of tables
    tables <- SCDB::get_tables(conn, pattern) |>
      tidyr::unite("db_table_id", tidyselect::any_of(c("catalog", "schema", "table")), sep = ".",
                   na.rm = TRUE, remove = FALSE)

    # Determine pattern to delete tables with
    ds_table_pattern <- SCDB::id(paste(c(schema, purrr::pluck(pattern, .default = "*")), collapse = "."), conn) |>
      as.character()
    ds_context <- stringr::str_remove(ds_table_pattern, r"{\.[^\.]+$}")


    tables_to_delete <- tables |>
      dplyr::filter(stringr::str_detect(.data$db_table_id, ds_table_pattern)) |>
      dplyr::select(!"db_table_id")


    # Check if logs is in the table, if yes, all tables must be deleted
    if ("logs" %in% tables_to_delete$table &&
        !identical(dplyr::select(tables_to_delete, !"db_table_id"), SCDB::get_tables(conn, ds_context))) {
      stop(glue::glue("'{schema}.logs' set to delete. Can only delete if entire feature store is dropped."))
    }

    if (nrow(tables_to_delete) > 0) {
      tables_to_delete |>
        dplyr::group_by(dplyr::row_number()) |>
        dplyr::group_map(SCDB::id) |>
        purrr::walk(~ DBI::dbRemoveTable(conn, .))
    }

    # Delete from logs
    log_table_id <- SCDB::id(paste(c(schema, "logs"), collapse = "."), conn)

    if (SCDB::table_exists(conn, log_table_id)) {

      log_records_to_delete <- SCDB::get_table(conn, log_table_id) |>
        SCDB::filter_keys(tables_to_delete, by = colnames(tables_to_delete), copy = TRUE)

      dplyr::rows_delete(
        dplyr::tbl(conn, log_table_id, check_from = FALSE),
        log_records_to_delete,
        by = colnames(log_records_to_delete),
        in_place = TRUE,
        unmatched = "ignore"
      )
    }
  }
}
