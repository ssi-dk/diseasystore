# This file contains reexported functions from the internal SSI package 'mg'.
# These dependencies should either be removed or ported fully to the diseasystore package



#' Gets the available tables
#'
#' @template conn
#' @param pattern A regex pattern with which to subset the returned tables
#' @return A data.frame containing table names in the DB
#' @export
mg_get_tables <- function(conn, pattern = NULL) {

  # Check arguments
  checkmate::assert_class(conn, "DBIConnection")
  checkmate::assert_character(pattern, null.ok = TRUE)

  # Retrieve all objects in conn
  objs <- DBI::dbListObjects(conn) |>
    dplyr::select(table)

  # purrr::map fails if .x is empty, avoid by returning early
  if (mg_nrow(objs) == 0) return(data.frame(schema = character(), table = character()))

  tables <- purrr::map(
    # For each top-level object (except tables)...
    objs$table, \(.x) {
      if (names(.x@name) == "table") {
        return(data.frame(schema = NA_character_, table = .x@name["table"]))
      }

      # ...retrieve all tables
      DBI::dbListObjects(conn, .x) |>
        dplyr::pull(table) |>
        purrr::map(\(.y) data.frame(schema = .x@name, table = .y@name["table"])) |>
        purrr::reduce(rbind.data.frame)
    }) |>
    purrr::reduce(rbind.data.frame)

  # Skip dbplyr temporary tables
  tables <- dplyr::filter(tables, !startsWith(.data$table, "dbplyr_"))

  # Skip PostgreSQL metadata tables
  if (inherits(conn, "PqConnection")) {
    tables <- dplyr::filter(tables, dplyr::case_when(
      is.na(schema) ~ TRUE,
      .data$schema == "information_schema" ~ FALSE,
      grepl("^pg_.*", .data$schema) ~ FALSE,
      TRUE ~ TRUE))
  }

  # Subset if pattern is given
  if (!is.null(pattern)) {
    tables <- subset(tables, grepl(pattern, table))
  }

  # Remove empty schemas
  tables <- dplyr::mutate(tables, schema = dplyr::if_else(.data$schema == "", NA, .data$schema))

  row.names(tables) <- NULL  # Reset row names
  return(tables)
}



#' Gets a named table from a given schema
#'
#' @template conn
#' @templateVar miss TRUE
#' @template db_table_id
#' @param slice_ts
#'   If set different from NA (default), the returned data looks as on the given date.
#'   If set as NULL, all data is returned
#' @param include_slice_info
#'   Default FALSE.
#'   If set TRUE, the history columns "checksum", "from_ts", "until_ts" are returned also
#' @return
#'   A "lazy" dataframe (tbl_lazy) generated using dbplyr
#' @export
mg_get_table <- function(conn, db_table_id = NULL, slice_ts = NA, include_slice_info = FALSE) {

  # Check arguments
  checkmate::assert_class(conn, "DBIConnection")
  mg_assert_id_like(db_table_id, null.ok = TRUE)
  mg_assert_timestamp_like(slice_ts, null.ok = TRUE)
  checkmate::assert_logical(include_slice_info)

  # Get tables in db schema
  if (is.null(db_table_id)) {
    print("Select one the following tables:")
    return(mg_get_tables(conn))
  }

  if (is.character(db_table_id)) db_table_id <- mg_id(db_table_id, conn = conn)

  # Ensure existence of table
  if (!mg_table_exists(conn, db_table_id)) {
    db_table_name_str <- paste(c(purrr::pluck(db_table_id, "name", "schema"),
                                 purrr::pluck(db_table_id, "name", "table")),
                               collapse = ".")
    stop(glue::glue("Table {db_table_name_str} is not found!"))
  }

  # Look-up table in DB
  q <- dplyr::tbl(conn, db_table_id)

  # Check whether data mg_is.historical
  if (mg_is.historical(q) && !is.null(slice_ts)) {

    # Filter based on date
    if (is.na(slice_ts)) {
      q <- dplyr::filter(q, is.na(.data$until_ts)) # Newest data
    } else {
      q <- mg_slice_time(q, slice_ts)
    }

    # Remove history columns
    if (!include_slice_info) {
      q <- dplyr::select(q, !tidyselect::any_of(c("from_ts", "until_ts", "checksum")))
    }
  }

  return(q)
}



#' Check if table exists in db
#'
#' @template conn
#' @template db_table_id
#' @export
mg_table_exists <- function(conn, db_table_id) {

  # Check arguments
  mg_assert_id_like(db_table_id)

  if (inherits(db_table_id, "Id")) {
    db_name <- attr(db_table_id, "name")
    db_schema <- purrr::pluck(db_name, "schema", .default = NA_character_)
    db_table  <- purrr::pluck(db_name, "table")
    db_table_id <- paste0(purrr::discard(c(db_schema, db_table), is.na), collapse = ".")
  }

  # Determine matches in the existing tables
  n_matches <- mg_get_tables(conn) |>
    tidyr::unite("db_table_id", "schema", "table", sep = ".", na.rm = TRUE) |>
    dplyr::filter(db_table_id == !!db_table_id) |>
    mg_nrow()

  if (n_matches >= 2) stop("Edge case detected. Cannot determine if table exists!")
  return(n_matches == 1)
}



#' Convenience function for DBI::Id
#'
#' @template db_table_id
#' @template conn
#' @seealso [DBI::Id] which this function wraps.
#' @export
mg_id <- function(db_table_id, conn = NULL) {

  # Check if already Id
  if (inherits(db_table_id, "Id")) return(db_table_id)

  # Check arguments
  checkmate::assert_character(db_table_id)

  # SQLite does not have schemas
  if (inherits(conn, "SQLiteConnection")) {
    return(DBI::Id(table = db_table_id))
  }

  if (stringr::str_detect(db_table_id, "\\.")) {
    db_name <- stringr::str_split_1(db_table_id, "\\.")
    db_schema <- db_name[1]
    db_table  <- db_name[2]
  } else {
    db_schema <- NULL
    db_table <- db_table_id
  }

  DBI::Id(schema = db_schema, table = db_table)
}


#' Slices a data object based on time / date
#'
#' @template .data
#' @param slice_ts The time / date to slice by
#' @param from_ts  The name of the column in .data specifying valid from time (note: must be unquoted)
#' @param until_ts The name of the column in .data specifying valid until time (note: must be unquoted)
#' @template .data_return
#' @export
mg_slice_time <- function(.data, slice_ts, from_ts = from_ts, until_ts = until_ts) {

  # Check arguments
  mg_assert_data_like(.data)
  mg_assert_timestamp_like(slice_ts)
  # TODO: How to checkmate from_ts and until_ts?

  from_ts  <- dplyr::enquo(from_ts)
  until_ts <- dplyr::enquo(until_ts)
  .data <- .data |>
    dplyr::filter(is.na({{until_ts}}) | slice_ts < {{until_ts}},
                  {{from_ts}} <= slice_ts)
  return(.data)
}



#' SQL Joins
#'
#' @name joins
#'
#' @description Overloads the dplyr *_join to accept an na_by argument.
#' By default, joining using SQL does not match on NA / NULL.
#' dbplyr has the option "na_matches = na" to match on NA / NULL but this is very inefficient
#' This function does the matching more efficiently.
#' If a column contains NA / NULL, give the argument to na_by to match during the join
#' If no na_by is given, the function defaults to using dplyr::*_join
#' @inheritParams dplyr::left_join
#' @param na_by columns that should match on NA
#' @seealso [dplyr::mutate-joins] which this function wraps.
#' @seealso [dbplyr::join.tbl_sql] which this function wraps.
#' @export
mg_inner_join <- function(x, y, by = NULL, na_by = NULL, ...) {

  # Check arguments
  mg_assert_data_like(x)
  mg_assert_data_like(y)
  checkmate::assert_character(by, null.ok = TRUE)
  checkmate::assert_character(na_by, null.ok = TRUE)

  if (is.null(na_by)) {
    if (inherits(x, "tbl_dbi") || inherits(y, "tbl_dbi")) mg_join_warn()
    return(dplyr::inner_join(x, y, by = by, ...))
  } else {
    mg_join_warn_experimental()
    sql_on <- mg_join_na_sql(by, na_by)
    renamer <- mg_select_na_sql(x, y, by, na_by)
    return(dplyr::inner_join(x, y, suffix = c(".x", ".y"), sql_on = sql_on, ...) |>
             dplyr::rename(!!renamer) |>
             dplyr::select(tidyselect::all_of(names(renamer))))
  }
}



# This function generates a much faster sql statement for NA join compared to dbplyr's _join with na_matches = "na".
mg_join_na_sql <- function(by, na_by) {
  sql_on <- ""
  if (!missing(by)) {
    for (i in seq_along(by)) {
      sql_on <- paste0(sql_on, '"LHS"."', by[i], '" = "RHS"."', by[i], '"')
      if (i < length(by) || !missing(na_by)) {
        sql_on <- paste(sql_on, "\nAND ")
      }
    }
  }

  if (!missing(na_by)) {
    for (i in seq_along(na_by)) {
      sql_on <- paste0(sql_on, '("LHS"."', na_by[i], '" IS NOT DISTINCT FROM "RHS"."', na_by[i], '")')
      if (i < length(na_by)) {
        sql_on <- paste(sql_on, "\nAND ")
      }
    }
  }

  return(sql_on)
}


# Get colnames from
mg_select_na_sql <- function(x, y, by, na_by, left = TRUE) {

  all_by <- c(by, na_by) # Variables to be common after join
  cxy <- dplyr::setdiff(dplyr::intersect(colnames(x), colnames(y)), all_by)   # Duplicate columns after join
  cx  <- dplyr::setdiff(colnames(x), colnames(y)) # Variables only in x
  cy  <- dplyr::setdiff(colnames(y), colnames(x)) # Variables only in y

  vars <- list(all_by, cx, cy, cxy, cxy)

  renamer <- \(suffix) suffix |> purrr::map(~ purrr::partial(\(x, suffix) paste0(x, suffix), suffix = .))

  sql_select <- vars |>
    purrr::map2(renamer(list(ifelse(left, ".x", ".y"), "", "", ".x", ".y")), ~ purrr::map(.x, .y)) |>
    purrr::map(~ purrr::reduce(., c, .init = character(0))) |>
    purrr::reduce(c)

  sql_names <- vars |>
    purrr::map2(renamer(list("", "", "", ".x", ".y")), ~ purrr::map(.x, .y)) |>
    purrr::map(~ purrr::reduce(., c, .init = character(0))) |>
    purrr::reduce(c)

  names(sql_select) <- sql_names

  return(sql_select)
}



#' A warning to users that SQL does not match on NA by default
mg_join_warn <- function() {
  if (testthat::is_testing() || !interactive()) return()
  if (identical(parent.frame(n = 2), globalenv())) {
    rlang::warn(paste("*_joins in database-backend does not match NA by default.\n",
                      "If your data contains NA, the columns with NA values must be supplied to \"na_by\",",
                      "or you must specifiy na_matches = \"na\""),
                .frequency = "once", .frequency_id = "*_join NA warning")
  }
}



#' A warning to users that SQL does not match on NA by default
mg_join_warn_experimental <- function() {
  if (testthat::is_testing() || !interactive()) return()
  if (identical(parent.frame(n = 2), globalenv())) {
    rlang::warn("*_joins with na_by is stil experimental. Please report issues to rassky",
                .frequency = "once", .frequency_id = "*_join NA warning")
  }
}



#' Combine any number of sql queries, where each has their own time axis of
#' validity (valid_from and valid_until)
#'
#' @description
#' The function "interlaces" the queries and combines their validity time axes
#' onto a single time axis
#'
#' @param tables    A list(!) of tables you want to combine is supplied here as
#'                  lazy_queries.
#' @param by        The (group) variable to merge by
#' @param colnames  If the time axes of validity is not called "valid_to" and
#'                  "valid_until" inside each lazy_query, you can specify their
#'                  names by supplying the arguments as a list
#'                  (e.g. c(t1.from = "\<colname\>", t2.until = "\<colname\>").
#'                  colnames must be named in same order as as given in tables
#'                  (i.e. t1, t2, t3, ...).
#' @return          The combination of input queries with a single, interlaced
#'                  valid_from / valid_until time axis
#' @export
mg_interlace_sql <- function(tables, by = NULL, colnames = NULL) {

  # Check arguments
  checkmate::assert_character(by)
  # TODO: how to checkmate tables and colnames?

  # Check edgecase
  if (length(tables) == 1) return(purrr::pluck(tables, 1))

  # Parse inputs for colnames .from / .to columns
  from_cols <- seq_along(tables) |>
    purrr::map_chr(\(t) paste0("t", t, ".from")) |>
    purrr::map_chr(\(col) ifelse(col %in% names(colnames), colnames[col], "valid_from"))

  until_cols <- seq_along(tables) |>
    purrr::map_chr(\(t) paste0("t", t, ".until")) |>
    purrr::map_chr(\(col) ifelse(col %in% names(colnames), colnames[col], "valid_until"))


  # Rename valid_from / valid_until columns
  tables <- tables |>
    purrr::map2(from_cols,  \(table, from_col)  table |> dplyr::rename(valid_from  = !!from_col)) |>
    purrr::map2(until_cols, \(table, until_col) table |> dplyr::rename(valid_until = !!until_col))


  # Get all changes to valid_from / valid_until
  q1 <- tables |> purrr::map(\(table) table |>
                               dplyr::select(tidyselect::all_of(by), "valid_from"))
  q2 <- tables |> purrr::map(\(table) table |>
                               dplyr::select(tidyselect::all_of(by), "valid_until") |>
                               dplyr::rename(valid_from = "valid_until"))
  t <- union(q1, q2) |> purrr::reduce(union)

  # Sort and find valid_until in the combined validities
  t <- t |>
    dplyr::group_by(dplyr::across(tidyselect::all_of(by))) |>
    dbplyr::window_order(.data$valid_from) |>
    dplyr::mutate(
      .row_number_id = dplyr::if_else(is.na(.data$valid_from),  # Some DB backends considers NULL to be the
                                      dplyr::n(),               # smallest, so we need to adjust for that
                                      dplyr::row_number() - ifelse(is.na(dplyr::first(.data$valid_from)), 1, 0)))

  t <- dplyr::left_join(t |>
                          dplyr::filter(.data$.row_number_id < dplyr::n()),
                        t |>
                          dplyr::filter(.data$.row_number_id > 1) |>
                          dplyr::mutate(.row_number_id = .data$.row_number_id - 1) |>
                          dplyr::rename("valid_until" = "valid_from"),
                        by = c(by, ".row_number_id")) |>
    dplyr::select(!".row_number_id") |>
    dplyr::ungroup() |>
    dplyr::compute()


  # Merge data onto the new validities using non-equi joins
  joiner <- \(.data, table) .data |>
    dplyr::left_join(table,
                     suffix = c("", ".tmp"),
                     sql_on = paste0(
                       '"LHS"."', by, '" = "RHS"."', by, '" AND
                        "LHS"."valid_from"  >= "RHS"."valid_from" AND
                       ("LHS"."valid_until" <= "RHS"."valid_until" OR "RHS"."valid_until" IS NULL)')) |>
    dplyr::select(!tidyselect::ends_with(".tmp")) |>
    dplyr::relocate(tidyselect::starts_with("valid_"), .after = tidyselect::everything())

  return(purrr::reduce(tables, joiner, .init = t))
}



#' Computes an MD5 checksum from columns
#'
#' @name mg_digest_to_checksum
#'
#' @template .data
#' @param col Name of the column to put the checksums in
#' @param warn Flag to warn if target column already exists in data
#' @param exclude Columns to exclude from the checksum generation
#'
#' @importFrom rlang `:=`
#' @export
mg_digest_to_checksum <- function(.data, col = "checksum", exclude = NULL, warn = TRUE) {

  # Check arguments
  checkmate::assert_character(col)
  checkmate::assert_logical(warn)

  if (as.character(dplyr::ensym(col)) %in% colnames(.data) && warn) {
    warning("Column ",
            as.character(dplyr::ensym(col)),
            " already exists in data and will be overwritten!")
  }

  colnames <- .data |>
    dplyr::select(!tidyselect::any_of(c(col, exclude))) |>
    colnames()

  .data <- .data |>
    dplyr::select(!tidyselect::any_of(col)) |>
    dplyr::mutate(dplyr::across(tidyselect::all_of(colnames), paste, .names = "{.col}.__chr"))

  mg_digest_to_checksum_internal(.data, col)
}


#'
# It seems we need to do more hacking since
# @importFrom openssl md5 does not work in the below usecase.
# defining md5 here succesfully causes local objects to use the openssl md5 function
# and remote objects to use their own md5 functions.
md5 <- openssl::md5


#' Some backends have native md5 support, these use this function
#' @rdname digest_internal
#' @importFrom rlang `:=`
mg_digest_to_checksum_native_md5 <- function(.data, col) {

  .data <- .data |>
    tidyr::unite(!!col, tidyselect::ends_with(".__chr"), remove = TRUE) |>
    dplyr::mutate(dplyr::across(tidyselect::all_of(col), md5))

  return(.data)
}


#' @name digest_internal
#' @template .data
#' @param col The name of column the checksums will be placed in
mg_digest_to_checksum_internal <- function(.data, col) {
  UseMethod("mg_digest_to_checksum_internal")
}


#' @rdname digest_internal
#' @importFrom rlang `:=` .data
mg_digest_to_checksum_internal.default <- function(.data, col) {

  # Compute checksums locally then join back onto original data
  checksums <- .data |>
    dplyr::collect() |>
    tidyr::unite(col, tidyselect::ends_with(".__chr")) |>
    dplyr::transmute(id__ = dplyr::row_number(),
                     checksum = openssl::md5({{ col }}))

  .data <- .data |>
    dplyr::mutate(id__ = dplyr::row_number()) |>
    dplyr::left_join(checksums, by = "id__", copy = TRUE) |>
    dplyr::select(!c(tidyselect::ends_with(".__chr"), "id__"))

  return(.data)
}


#' @rdname digest_internal
mg_digest_to_checksum_internal.tbl_PqConnection <- mg_digest_to_checksum_native_md5


#' @rdname digest_internal
mg_digest_to_checksum_internal.data.frame       <- mg_digest_to_checksum_native_md5


#' @rdname digest_internal
mg_digest_to_checksum_internal.tibble           <- mg_digest_to_checksum_native_md5




#' Create a historical table from input data
#'
#' @name mg_create_table
#'
#' @template .data
#' @template conn
#' @template db_table_id
#' @param temporary Should the table be created as a temporary table?
#' @param ... Other arguments passed to [DBI::dbCreateTable()]
#' @returns Invisibly returns the table as it looks on the destination (or locally if conn is NULL)
#' @export
mg_create_table <- function(.data, conn = NULL, db_table_id = NULL, temporary = TRUE, ...) {

  checkmate::assert_class(.data, "data.frame")
  checkmate::assert_class(conn, "DBIConnection", null.ok = TRUE)
  mg_assert_id_like(db_table_id)

  # Assert unique column names (may cause unexpected mg_getTableSignature results)
  checkmate::assert_character(names(.data), unique = TRUE)

  # Convert db_table_id to mg_id (mg_id() returns early if this is the case)
  if (!is.null(db_table_id)) { # TODO: db_table_name vs db_table_id
    db_table_id <- mg_id(db_table_id, conn = conn)
  } else {
    db_table_id <- deparse(substitute(.data))
  }

  stopifnot("checksum/from_ts/until_ts column(s) already exist(s) in .data!" != any(
    c("checksum", "from_ts", "in_ts") %in% colnames(.data)))

  # Add "metadata" columns to .data
  .data <- .data |>
    dplyr::mutate(checksum = NA_character_,
                  from_ts  = as.POSIXct(NA_real_),
                  until_ts = as.POSIXct(NA_real_),
                  .after = tidyselect::everything())

  # Early return if there is no connection to push to
  if (is.null(conn)) return(invisible(.data))

  # Create the table on the remote and return the table
  stopifnot("Table already exists!" = !mg_table_exists(conn, db_table_id))
  DBI::dbCreateTable(conn = conn,
                     name = db_table_id,
                     fields = mg_getTableSignature(.data = .data, conn = conn),
                     temporary = temporary,
                     ...)

  invisible(dplyr::tbl(conn, db_table_id))
}



# TODO: some development comments

#' @importFrom methods setGeneric
methods::setGeneric("mg_getTableSignature", # TODO: Why camelCase here?
           function(.data, conn = NULL) standardGeneric("mg_getTableSignature"),
           signature = "conn")


#'
methods::setMethod("mg_getTableSignature", "DBIConnection", function(.data, conn) {
  # Define the column types to be updated based on backend class
  col_types <- DBI::dbDataType(conn, .data)

  backend_coltypes <- list(
    "PqConnection" = c(
      checksum = "TEXT",
      from_ts  = "TIMESTAMP",
      until_ts = "TIMESTAMP"
    ),
    "SQLiteConnection" = c(
      checksum = "TEXT",
      from_ts  = "TEXT",
      until_ts = "TEXT"
    )
  )

  # Update columns with indices instead of names to avoid conflicts
  special_cols <- backend_coltypes[[class(conn)]]
  special_indices <- (1 + length(.data) - length(special_cols)):length(.data)

  return(replace(col_types, special_indices, special_cols))
})


#'
methods::setMethod("mg_getTableSignature", "NULL", function(.data, conn) {
  # Emulate product of DBI::dbDataType
  signature <- dplyr::summarise(.data, dplyr::across(tidyselect::everything(), ~ class(.)[1]))

  stats::setNames(as.character(signature), names(signature))

  return(signature)
})



#' Update a historical table
#' @template .data_dbi
#' @template conn
#' @template db_table
#' @param timestamp
#'   A timestamp (POSIXct) with which to update from_ts/until_ts columns
#' @template filters
#' @param message
#'   A message to add to the log-file (useful for supplying metadata to the log)
#' @param tic
#'   A timestamp when computation began. If not supplied, it will be created at call-time.
#'   (Used to more accurately convey how long runtime of the update process has been)
#' @template log_path
#' @template log_table_id
#' @param enforce_chronological_order
#'   A logical that controls whether or not to check if timestamp of update is prior to timestamps in the DB
#' @return NULL
#' @seealso mg_filter_keys
#' @importFrom rlang .data
#' @export
mg_update_snapshot <- function(.data, conn, db_table, timestamp, filters = NULL, message = NULL, tic = Sys.time(),
                            log_path = getOption("mg.log_path"), log_table_id = getOption("mg.log_table_id"),
                            enforce_chronological_order = TRUE) {

  # Check arguments
  checkmate::assert_class(.data, "tbl_dbi")
  checkmate::assert_class(conn, "DBIConnection")
  mg_assert_dbtable_like(db_table)
  mg_assert_timestamp_like(timestamp)
  checkmate::assert_class(filters, "tbl_dbi", null.ok = TRUE)
  checkmate::assert_character(message, null.ok = TRUE)
  mg_assert_timestamp_like(tic)
  checkmate::assert_logical(enforce_chronological_order)


  # If db_table is given as str, fetch the actual table
  # If the table does not exist, create an empty table using the signature of the incoming data
  if (is.character(db_table)) {

    db_table_name <- db_table
    db_table_id <- mg_id(db_table, conn)

    if (mg_table_exists(conn, db_table)) {
      db_table <- dplyr::tbl(conn, db_table_id)
    } else {
      db_table <- mg_create_table(dplyr::collect(utils::head(.data, 0)), conn, db_table_id, temporary = FALSE)
    }
  } else {
    db_table_id <- dbplyr::remote_name(db_table)
    db_table_name <- db_table_id |> as.character() |> stringr::str_remove_all('\"')
  }

  # Initialize logger
  logger <- mg_Logger$new(
    db_tablestring = db_table_name,
    log_table_id = log_table_id,
    log_conn = conn,
    log_path = log_path,
    ts = timestamp,
    start_time = tic
  )

  logger$log_to_db(start_time = !!mg_db_timestamp(tic, conn))
  logger$log_info("Started", tic = tic) # Use input time in log

  # Add message to log (if given)
  if (!is.null(message)) {
    logger$log_to_db(message = message)
    logger$log_info("Message:", message, tic = tic)
  }


  # Opening checks
  if (!mg_is.historical(db_table)) {
    logger$log_to_db(success = FALSE, end_time = !!mg_db_timestamp(tic, conn))
    logger$log_error("Table does not seem like a historical table", tic = tic) # Use input time in log
  }

  if (!setequal(colnames(.data), colnames(
    dplyr::select(db_table, !c("checksum", "from_ts", "until_ts")))
  )) {
    logger$log_to_db(success = FALSE, end_time = !!mg_db_timestamp(tic, conn))
    logger$log_error("Columns do not match!\n",
                     "Table columns:\n",
                     paste(colnames(dplyr::select(db_table, !tidyselect::any_of(c("checksum", "from_ts", "until_ts")))),
                           collapse = ", "),
                      "\nInput columns:\n",
                     paste(colnames(.data), collapse = ", "), tic = tic) # Use input time in log
  }

  logger$log_to_db(schema = purrr::pluck(db_table_id@name, "schema"), table = purrr::pluck(db_table_id@name, "table"))
  logger$log_info("Parsing data for table", db_table_name, "started", tic = tic) # Use input time in log
  logger$log_to_db(date = !!mg_db_timestamp(timestamp, conn))
  logger$log_info("Given timestamp for table is", timestamp, tic = tic) # Use input time in log

  # Check for current update status
  db_latest <- db_table |>
    dplyr::summarize(max(.data$from_ts, na.rm = TRUE)) |>
    dplyr::pull() |>
    as.character() |>
    max("1900-01-01 00:00:00", na.rm = TRUE)

    # Convert timestamp to character to prevent inconsistent R behavior with date/timestamps
  timestamp <- strftime(timestamp)

  if (enforce_chronological_order && timestamp < db_latest) {
    logger$log_to_db(success = FALSE, end_time = !!mg_db_timestamp(tic, conn))
    logger$log_error("Given timestamp", timestamp, "is earlier than latest",
                     "timestamp in table:", db_latest, tic = tic) # Use input time in log
  }

  # Compute .data immediately to reduce runtime and compute checksum
  .data <- .data |>
    dplyr::ungroup() |>
    dplyr::select(
      colnames(dplyr::select(db_table, !tidyselect::any_of(c("checksum", "from_ts", "until_ts"))))
    ) |>
    mg_digest_to_checksum(col = "checksum", warn = TRUE) |>
    mg_filter_keys(filters) |>
    dplyr::compute()


  # Apply filter to current records
  db_table <- mg_filter_keys(db_table, filters)

  # Determine the next timestamp in the data (can be NA if none is found)
  next_timestamp <- min(
    db_table |>
      dplyr::filter(.data$from_ts  > timestamp) |>
      dplyr::summarize(next_timestamp = min(.data$from_ts, na.rm = TRUE)) |>
      dplyr::pull("next_timestamp"),
    db_table |>
      dplyr::filter(.data$until_ts > timestamp) |>
      dplyr::summarize(next_timestamp = min(.data$until_ts, na.rm = TRUE)) |>
      dplyr::pull("next_timestamp")) |>
    strftime()

  # Consider only records valid at timestamp (and apply the filter if present)
  db_table <- mg_slice_time(db_table, timestamp)

  # Count open rows at timestamp
  nrow_open <- mg_nrow(db_table)


  # Select only data with no until_ts and with different values in any fields
  logger$log_info("Deactivating records")
  if (nrow_open > 0) {
    to_remove <- dplyr::setdiff(dplyr::select(db_table, "checksum"),
                                dplyr::select(.data, "checksum")) |>
      dplyr::compute() # Something has changed in dbplyr (2.2.1) that makes this compute needed.
                       # Code that takes 20 secs with can be more than 30 minutes to compute without...

    nrow_to_remove <- mg_nrow(to_remove)

    # Determine from_ts and checksum for the records we need to deactivate
    to_remove <- to_remove |>
      dplyr::left_join(dplyr::select(db_table, "from_ts", "checksum"), by = "checksum") |>
      dplyr::mutate(until_ts = !!mg_db_timestamp(timestamp, conn))

  } else {
    nrow_to_remove <- 0
  }
  logger$log_info("After to_remove")



  to_add <- dplyr::setdiff(.data, dplyr::select(db_table, colnames(.data))) |>
    dplyr::mutate(from_ts  = !!mg_db_timestamp(timestamp, conn),
                  until_ts = !!mg_db_timestamp(next_timestamp, conn))

  nrow_to_add <- mg_nrow(to_add)
  logger$log_info("After to_add")



  if (nrow_to_remove > 0) {
    dplyr::rows_update(x = dplyr::tbl(conn, db_table_id),
                       y = to_remove,
                       by = c("checksum", "from_ts"),
                       in_place = TRUE,
                       unmatched = "ignore")
  }

  logger$log_to_db(n_deactivations = nrow_to_remove) # Logs contains the aggregate number of added records on the day
  logger$log_info("Deactivate records count:", nrow_to_remove)
  logger$log_info("Adding new records")

  if (nrow_to_add > 0) {
    dplyr::rows_append(x = dplyr::tbl(conn, db_table_id),
                       y = to_add,
                       in_place = TRUE)
  }

  logger$log_to_db(n_insertions = nrow_to_add)
  logger$log_info("Insert records count:", nrow_to_add)


  # If several updates come in a single day, some records may have from_ts = until_ts.
  # We remove these records here
  redundant_rows <- dplyr::tbl(conn, db_table_id) |>
    dplyr::filter(.data$from_ts == .data$until_ts) |>
    dplyr::select("checksum", "from_ts")
  nrow_redundant <- mg_nrow(redundant_rows)

  if (nrow_redundant > 0) {
    dplyr::rows_delete(
      dplyr::tbl(conn, db_table_id),
      redundant_rows,
      by = c("checksum", "from_ts"),
      in_place = TRUE, unmatched = "ignore")
    logger$log_info("Doubly updated records removed:", nrow_redundant)
  }

  # If chronological order is not enforced, some records may be split across several records
  # checksum is the same, and from_ts / until_ts are continuous
  # We collapse these records here
  if (!enforce_chronological_order) {
    redundant_rows <- dplyr::tbl(conn, db_table_id) |>
      mg_filter_keys(filters)

    redundant_rows <- dplyr::inner_join(
        redundant_rows,
        redundant_rows |> dplyr::select("checksum", "from_ts", "until_ts"),
        suffix = c("", ".p"),
        sql_on = '"RHS"."checksum" = "LHS"."checksum" AND "LHS"."until_ts" = "RHS"."from_ts"'
      ) |>
      dplyr::select(!"checksum.p")

    redundant_rows_to_delete <- redundant_rows |>
      dplyr::transmute(.data$checksum, from_ts = .data$from_ts.p) |>
      dplyr::compute()

    redundant_rows_to_update <- redundant_rows |>
      dplyr::transmute(.data$checksum, from_ts = .data$from_ts, until_ts = .data$until_ts.p) |>
      dplyr::compute()

    if (mg_nrow(redundant_rows_to_delete) > 0) {
      dplyr::rows_delete(x = dplyr::tbl(conn, db_table_id),
                         y = redundant_rows_to_delete,
                         by = c("checksum", "from_ts"),
                         in_place = TRUE,
                         unmatched = "ignore")
    }

    if (mg_nrow(redundant_rows_to_update) > 0) {
      dplyr::rows_update(x = dplyr::tbl(conn, db_table_id),
                         y = redundant_rows_to_update,
                         by = c("checksum", "from_ts"),
                         in_place = TRUE,
                         unmatched = "ignore")
      logger$log_info("Continous records collapsed:", mg_nrow(redundant_rows_to_update))
    }

  }

  toc <- Sys.time()
  logger$log_to_db(end_time = !!mg_db_timestamp(toc, conn),
                   duration = !!format(round(difftime(toc, tic), digits = 2)), success = TRUE)
  logger$log_info("Finished processing for table", db_table_name, tic = toc)

}



#' Filters .data according to all records in the filter
#'
#' @description
#' If filter = NULL, not filtering is done
#' If filter is different from NULL, the .data is filtered by a mg_inner_join using all columns of the filter:
#' \code{mg_inner_join(.data, filter, by = colnames(filter))}
#'
#' by and na_by can overwrite the mg_inner_join columns used in the filtering
#'
#' @template .data
#' @template filters
#' @param by      passed to mg_inner_join if different from NULL
#' @param na_by   passed to mg_inner_join if different from NULL
#' @template .data_return
#' @export
mg_filter_keys <- function(.data, filters, by = NULL, na_by = NULL) {

  # Check arguments
  mg_assert_data_like(.data)
  mg_assert_data_like(filters, null.ok = TRUE)
  checkmate::assert_character(by, null.ok = TRUE)
  checkmate::assert_character(na_by, null.ok = TRUE)

  if (is.null(filters)) {
    return(.data)
  } else {
    if (is.null(by) && is.null(na_by)) {
      # Determine key types
      key_types <- filters |>
        dplyr::ungroup() |>
        dplyr::summarise(dplyr::across(.fns = ~ any(is.na(.), na.rm = TRUE))) |>
        tidyr::pivot_longer(tidyselect::everything(), names_to = "column_name", values_to = "is_na")

      by    <- key_types |> dplyr::filter(!.data$is_na) |> dplyr::pull("column_name")
      na_by <- key_types |> dplyr::filter(.data$is_na)  |> dplyr::pull("column_name")

      if (length(by) == 0)    by    <- NULL
      if (length(na_by) == 0) na_by <- NULL
    }
    return(mg_inner_join(.data, filters, by = by, na_by = na_by))
  }
}



#' Determine the type of timestamps the DB supports
#' @name mg_db_timestamp
#' @param timestamp The timestamp to be transformed to the DB type. Can be character.
#' @param conn A `DBIConnection` to the DB where the timestamp should be stored
mg_db_timestamp <- function(timestamp, conn) {
  UseMethod("mg_db_timestamp", conn)
}


#' @rdname mg_db_timestamp
mg_db_timestamp.default <- function(timestamp, conn) {
  return(dbplyr::translate_sql(as.POSIXct(!!timestamp), con = conn))
}


#' @rdname mg_db_timestamp
mg_db_timestamp.SQLiteConnection <- function(timestamp, conn) {
  if (is.na(timestamp)) {
    dbplyr::translate_sql(NA_character_, con = conn)
  } else {
    dbplyr::translate_sql(!!strftime(timestamp), con = conn)
  }
}



#' Checks if table contains historical data
#'
#' @template .data
#' @return TRUE if .data contains the columns: "checksum", "from_ts", and "until_ts". FALSE otherwise
#' @export
mg_is.historical <- function(.data) { # nolint: object_name_linter

  # Check arguments
  mg_assert_data_like(.data)

  return(all(c("checksum", "from_ts", "until_ts") %in% colnames(.data)))
}




#' mg_nrow() but also works on remote tables
#'
#' @param .data lazy_query to parse
#' @return The number of records in the object
#' @export
mg_nrow <- function(.data) {
  if (inherits(.data, "tbl_dbi")) {
    return(dplyr::pull(dplyr::count(dplyr::ungroup(.data))))
  } else {
    return(base::nrow(.data))
  }
}



#' @title mg_Logger
#' @description
#' Create an object for logging database operations
#'
#' @param db_tablestring A string specifying the table being updated
#' @template log_table_id
#' @template log_path
#' @param ts A timestamp describing the data being processed (`r "\U2260"` current time)
#' @param start_time The time at which data processing was started (defaults to [Sys.time()])
#'
#' @export
mg_Logger <- R6::R6Class("mg_Logger", #nolint: object_name_linter
  public = list(

    #' @field log_path (`character(1)`)\cr
    #' A directory where log file is written (if this is not NULL). Defaults to `getOption("mg.log_path")`.
    log_path = NULL,

    #' @field log_filename (`character(1)`)\cr
    #' The name (basename) of the log file.
    log_filename = NULL,

    #' @field log_tbl
    #' The DB table used for logging. Class is connection-specific, but inherits from `tbl_dbi`.
    log_tbl = NULL,

    #' @field start_time (`POSIXct(1)`)\cr
    #' The time at which data processing was started.
    start_time = NULL,

    #' @description
    #' Create a new mg_Logger object
    #' @param log_conn A database connection inheriting from `DBIConnection`
    initialize = function(db_tablestring = NULL,
                          log_table_id   = getOption("mg.log_table_id"),
                          log_conn = NULL,
                          log_path = getOption("mg.log_path"),
                          ts = NULL,
                          start_time = Sys.time()
                          ) {

      # Initialize logger
      coll <- checkmate::makeAssertCollection()
      checkmate::assert_character(db_tablestring, add = coll)
      mg_assert_id_like(log_table_id, null.ok = TRUE, add = coll)
      checkmate::assert_class(log_conn, "DBIConnection", null.ok = TRUE, add = coll)
      checkmate::assert_character(log_path, null.ok = TRUE, add = coll)
      mg_assert_timestamp_like(ts, add = coll)
      checkmate::assert_posixct(start_time, add = coll)
      checkmate::reportAssertions(coll)

      private$ts <- ts
      self$start_time <- start_time
      lockBinding("start_time", self)

      if (!is.null(log_table_id)) {
        self$log_tbl <- mg_create_logs_if_missing(log_table_id, log_conn)
      }
      private$log_conn <- log_conn

      self$log_path <- log_path
      private$db_tablestring <- db_tablestring
      self$log_filename <- private$generate_filename()
      lockBinding("log_filename", self)

      # Create a line in log DB for mg_Logger
      private$generate_log_entry()

      stopifnot("Log file for given timestamp already exists!" = !file.exists(file.path(self$log_path, self$log_filename)))
    },

    #' @description
    #' Write a line to log file
    #' @param ... `r log_dots <- "One or more character strings to be concatenated"; log_dots`
    #' @param tic The timestamp used by the log entry (default Sys.time())
    #' @param log_type `r log_type <- "A character string which describes the severity of the log message"; log_type`
    log_info = function(..., tic = Sys.time(), log_type = "INFO") {

      # Writes log file (if set)
      if (!is.null(self$log_path)) {
        sink(file = file.path(self$log_path, self$log_filename), split = TRUE, append = TRUE, type = "output")
      }

      cat(private$log_format(..., tic = tic, log_type = log_type), "\n", sep = "")

      if (!is.null(self$log_path)) sink()
    },

    #' @description Write a warning to log file and generate warning.
    #' @param ... `r log_dots`
    #' @param log_type `r log_type`
    log_warn = function(..., log_type = "WARNING") {
      self$log_info(..., log_type = log_type)
      warning(private$log_format(..., log_type = log_type))
    },

    #' @description Write an error to log file and stop execution
    #' @param ... `r log_dots`
    #' @param log_type `r log_type`
    log_error = function(..., log_type = "ERROR") {
      self$log_info(..., log_type = log_type)
      stop(private$log_format(..., log_type = log_type))
    },

    #' @description Write or update log table
    #' @param ... Name-value pairs with which to update the log table
    log_to_db = function(...) {
      if (is.null(self$log_tbl)) return()

      dplyr::rows_patch(
        x = self$log_tbl,
        y = dplyr::copy_to(private$log_conn, data.frame(log_file = self$log_filename), overwrite = TRUE) |>
          dplyr::mutate(...),
        by = "log_file",
        copy = TRUE,
        in_place = TRUE,
        unmatched = "ignore"
      )
    }
  ),
  private = list(

    db_tablestring = NULL,
    log_conn = NULL,
    ts = NULL,

    generate_filename = function() {
      # If we are not producing a file log, we provide a random string to key by
      if (is.null(self$log_path)) return(basename(tempfile(tmpdir = "", pattern = "")))

      start_format <- format(self$start_time, "%Y%m%d.%H%M")
      ts <- private$ts

      if (is.character(ts)) ts <- as.Date(ts)
      ts_format <- format(ts, "%Y_%m_%d")
      filename <- sprintf(
        "%s.%s.%s.log",
        start_format,
        ts_format,
        private$db_tablestring
      )

      return(filename)
    },


    generate_log_entry = function() {
      # Create a row for log in question
      if (!is.null(self$log_tbl)) {
        dplyr::rows_append(
          x = self$log_tbl,
          y = data.frame(log_file = self$log_filename),
          copy = TRUE,
          in_place = TRUE)
      }

      return()
    },

    log_format = function(..., tic = NULL, log_type = NULL) {
      ts_str <- if (is.null(tic)) {
        self$start_time
      } else {
        stringr::str_replace(format(tic, "%F %H:%M:%OS3", locale = "en"), "[.]", ",")
      }

      return(paste(ts_str, Sys.info()[["user"]], log_type, paste(...), sep = " - "))
    }
  )
)



#' Create a table with the mg log structure if it does not exists
#' @template conn
#' @param log_table A specification of where the logs should exist ("schema.table")
#' @export
mg_create_logs_if_missing <- function(log_table, conn) {

  checkmate::assert_class(conn, "DBIConnection")

  if (!mg_table_exists(conn, log_table)) {
    log_signature <- data.frame(date = as.POSIXct(NA),
                                schema = NA_character_,
                                table = NA_character_,
                                n_insertions = NA_integer_,
                                n_deactivations = NA_integer_,
                                start_time = as.POSIXct(NA),
                                end_time = as.POSIXct(NA),
                                duration = NA_character_,
                                success = NA,
                                message = NA_character_,
                                log_file = NA_character_) |>
      utils::head(0)

    DBI::dbWriteTable(conn, mg_id(log_table, conn), log_signature)
  }

  return(dplyr::tbl(conn, mg_id(log_table, conn)))
}

#' Provides mg_age_labels that follows the mg standard
#' @template age_cuts
#' @return A vector of labels with zero-padded numerics so they can be sorted easily
#' @export
mg_age_labels <- function(age_cuts) {
  checkmate::assert_numeric(age_cuts, any.missing = FALSE, lower = 0, unique = TRUE, sorted = TRUE)

  age_cuts <- age_cuts[age_cuts > 0 & is.finite(age_cuts)]
  width <- nchar(as.character(max(c(0, age_cuts))))
  stringr::str_c(
    stringr::str_pad(c(0, age_cuts), width, pad = "0"),
    c(rep("-", length(age_cuts)), "+"),
    c(stringr::str_pad(age_cuts - 1, width, pad = "0"), ""))
}



#' checkmate helper: Assert for "generic" db_table type
#' @param db_table Object to test if is of class "tbl_dbi" or character on form "schema.table"
#' @param ...      Parameters passed to checkmate::check_*
#' @param add      `AssertCollection` to add assertions to
#' @export
mg_assert_dbtable_like <- function(db_table, ..., add = NULL) {
  checkmate::assert(
    checkmate::check_character(db_table, pattern = r"{^\w*.\w*$}", ...),
    checkmate::check_class(db_table, "Id", ...),
    checkmate::check_class(db_table, "tbl_dbi", ...),
    add = add)
}



#' checkmate helper: Assert "generic" timestamp type
#' @param timestamp Object to test if is POSIX or character
#' @param ...       parameters passed to checkmate::check_*
#' @param add       `AssertCollection` to add assertions to
#' @export
mg_assert_timestamp_like <- function(timestamp, ..., add = NULL) {
  checkmate::assert(
    checkmate::check_posixct(timestamp, ...),
    checkmate::check_character(timestamp, ...),
    checkmate::check_date(timestamp, ...),
    add = add)
}



#' checkmate helper: Assert "generic" data.table/data.frame/tbl/tibble type
#' @param .data Object to test if is data.table, data.frame, tbl or tibble
#' @param ...   Parameters passed to checkmate::check_*
#' @param add   `AssertCollection` to add assertions to
#' @export
mg_assert_data_like <- function(.data, ..., add = NULL) {
  checkmate::assert(
    checkmate::check_class(.data, "tbl_dbi", ...),
    checkmate::check_data_frame(.data, ...),
    checkmate::check_data_table(.data, ...),
    checkmate::check_tibble(.data, ...),
    add = add)
}



#' checkmate helper: Assert for "generic" mg_id structure
#' @param mg_id   Object to test if is of class "Id" or character on form "schema.table"
#' @param ...  Parameters passed to checkmate::check_*
#' @param add `AssertCollection` to add assertions to
#' @export
mg_assert_id_like <- function(mg_id, ..., add = NULL) {
  checkmate::assert(
    checkmate::check_character(mg_id, ...),
    checkmate::check_class(mg_id, "Id", ...),
    add = add)
}

