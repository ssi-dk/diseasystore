#' Sets, queries and removes locks for db tables
#' @name db_locks
#' @description
#' This set of function adds a simple locking system to db tables.
#' * `add_table_lock` adds a record in the target_schema.lock table with the current time and R-session process id.
#' * `remove_table_lock` removes records in the target_schema.lock table with the target table and the
#'    R-session process id.
#' * `is_lock_owner` returns TRUE if the current process id (pid) matches the pid associated with the lock on db_table
#'    in target_schema.lock. If no lock is found, NULL is returned.
#' * `cleanup_locks` removes locks that are timed out
#' @param conn An object that inherits from DBIConnection (as generated by get_connection())
#' @param db_table Either a dplyr connection to target table or a specification of 'schema.table'
#' @param schema DB schema where lock table is / should be placed
#' @return Most are called for side effects. `is_lock_owner` returns the TRUE if the process can modify the table.
#' @examples
#' conn <- DBI::dbConnect(RSQLite::SQLite())
#'
#' is_lock_owner(conn, "test_table") # NULL
#'
#' add_table_lock(conn, "test_table")
#' is_lock_owner(conn, "test_table") # TRUE
#'
#' remove_table_lock(conn, "test_table")
#' is_lock_owner(conn, "test_table") # NULL
#'
#' DBI::dbDisconnect(conn)
#' @export
add_table_lock <- function(conn, db_table, schema = NULL) {

  # Determine lock table id
  lock_table_id <- SCDB::id(paste(c(schema, "locks"), collapse = "."), conn)

  # Create lock table if missing
  if (!SCDB::table_exists(conn, lock_table_id)) {
    dplyr::copy_to(conn,
                   data.frame("db_table" = character(0),
                              "lock_start" = numeric(0),
                              "pid" = numeric(0)),
                   lock_table_id, temporary = FALSE, unique_indexes = "db_table")
  }

  # Get a reference to the table
  lock_table <- dplyr::tbl(conn, lock_table_id)

  # We first delete old locks.
  cleanup_locks(conn, schema)

  # We then try to insert a lock, if none exists, our process ID (pid) will be assigned to the table
  # if one already exists, our insert will fail.
  tryCatch(
    dplyr::rows_insert(
      lock_table,
      data.frame("db_table" = db_table,
                 "pid" = Sys.getpid(),
                 "lock_start" = as.numeric(Sys.time())),
      by = "db_table", conflict = "ignore",
      in_place = TRUE, copy = TRUE
    ),
    error = function(e) {
      print(e)
    }
  )

  return()
}


#' @rdname db_locks
#' @export
remove_table_lock <- function(conn, db_table, schema = NULL) {

  # Determine lock table id
  lock_table_id <- SCDB::id(paste(c(schema, "locks"), collapse = "."), conn)

  # Create lock table if missing
  if (!SCDB::table_exists(conn, lock_table_id)) {
    return()
  }

  # Get a reference to the table
  lock_table <- dplyr::tbl(conn, lock_table_id)

  # Delete locks matching  our process ID (pid) and the given db_table
  dplyr::rows_delete(lock_table,
                     data.frame("db_table" = db_table,
                                "pid" = Sys.getpid()),
                     by = c("db_table", "pid"), unmatched = "ignore", in_place = TRUE, copy = TRUE)

  return()
}


#' @rdname db_locks
#' @export
is_lock_owner <- function(conn, db_table, schema = NULL) {

  # Determine lock table id
  lock_table_id <- SCDB::id(paste(c(schema, "locks"), collapse = "."), conn)

  # Create lock table if missing
  if (!SCDB::table_exists(conn, lock_table_id)) {
    return()
  }

  # Get a reference to the table
  lock_owner <- dplyr::tbl(conn, lock_table_id) |>
    dplyr::filter(.data$db_table == !!db_table) |>
    dplyr::pull("pid") |>
    as.integer()

  return(lock_owner == Sys.getpid())
}


#' @rdname db_locks
#' @importFrom rlang .data
cleanup_locks <- function(conn, schema = NULL) {

  # Determine lock table id
  lock_table_id <- SCDB::id(paste(c(schema, "locks"), collapse = "."), conn)

  # Create lock table if missing
  if (!SCDB::table_exists(conn, lock_table_id)) {
    return()
  }

  # Get a reference to the table
  lock_table <- dplyr::tbl(conn, lock_table_id)

  # Detect and delete old locks
  old_locks <- lock_table |>
    dplyr::filter(.data$lock_start < !!as.numeric(Sys.time()) - 60 * 30) |>
    dplyr::select("db_table")
  dplyr::rows_delete(lock_table, old_locks, by = "db_table", unmatched = "ignore", in_place = TRUE)

  return()
}