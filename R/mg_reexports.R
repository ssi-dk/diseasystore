# This file contains reexported functions from the internal SSI package 'mg'.
# These dependencies should either be removed or ported fully to the diseasystore package



#' Opens connection to the database
#'
#' Connects to the specified dbname of host:port using user and password from given arguments.
#' Certain drivers may use credentials stored in a file, such as ~/.pgpass (PostgreSQL)
#'
#' @param drv          An object that inherits from DBIDriver or an existing DBIConnection (default: RPostgres::Postgres())
#' @param host         Character string giving the ip of the host to connect to
#' @param port         Host port to connect to (numeric)
#' @param dbname       Name of the database located at the host
#' @param user         Username to login with
#' @param password     Password to login with
#' @param timezone     Sets the timezone of DBI::dbConnect()
#' @param timezone_out Sets the timezone_out of DBI::dbConnect()
#' @param ...          Additional parameters sent to DBI::dbConnect()
#' @return An object that inherits from DBIConnection driver specified in drv
#' @export
mg_get_connection <- function(drv = RPostgres::Postgres(),
                           host = NULL,
                           port = NULL,
                           dbname = NULL,
                           user = NULL,
                           password = NULL,
                           timezone = NULL,
                           timezone_out = NULL,
                           ...) {

  # Check arguments
  checkmate::assert_character(host, pattern = r"{^\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}$}", null.ok = TRUE)
  checkmate::assert_numeric(port, null.ok = TRUE)
  checkmate::assert_character(dbname,   null.ok = TRUE)
  checkmate::assert_character(user,     null.ok = TRUE)
  checkmate::assert_character(password, null.ok = TRUE)
  checkmate::assert_character(timezone, null.ok = TRUE)
  checkmate::assert_character(timezone_out, null.ok = TRUE)

  # Set PostgreSQL-specific options
  if (inherits(drv, "PqDriver")){
    if (is.null(timezone)) timezone <- Sys.timezone()
    if (is.null(timezone_out)) timezone_out <- timezone
  }

  # Check if connection can be established given these settings
  can_connect <- DBI::dbCanConnect(drv = drv, ...)
  if (!can_connect) stop("Could not connect to database with the given parameters: ", attr(can_connect, "reason"))

  conn <- DBI::dbConnect(drv = drv,
                         dbname = dbname,
                         host = host,
                         port = port,
                         user = user,
                         password = password,
                         timezone = timezone,
                         timezone_out = timezone_out,
                         ...,
                         bigint = "integer", # R has poor integer64 integration, which is the default return
                         check_interrupts = TRUE)

  # Check connection
  if (mg_nrow(mg_get_tables(conn)) == 0) {
    warn_str <- "No tables found. Check user permissions / database configuration"
    args <- c(as.list(environment()), list(...))
    set_args <- args[names(args) %in% c("dbname", "host", "port", "user", "password")]

    if (length(set_args) > 0){
      warn_str <- paste0(warn_str, ":\n  ")
      warn_str <- paste0(warn_str, paste(names(set_args), set_args, sep = ": ", collapse = "\n  "))
    }
    warning(warn_str)
  }

  return(conn)
}

#' Get the current schema of a DB connection
#'
#' @param .x A DBIConnection or lazy_query object
#' @return The current schema name, but defaults to "prod" instead of "public"
#' @export
mg_get_schema <- function(.x) {

  if (inherits(.x, "PqConnection")) {
    # Get schema from connection object
    schema <- DBI::dbGetQuery(.x, "SELECT CURRENT_SCHEMA()")$current_schema

  } else if (inherits(.x, "SQLiteConnection")) {
    return(NULL)
  } else if (inherits(.x, "tbl_dbi")) {
    # Get schema from a DBI object (e.g. lazy query)
    schema <- stringr::str_extract_all(dbplyr::remote_query(.x), '(?<=FROM \")[^"]*')[[1]]
    if (length(unique(schema)) > 1) {
      # Not sure if this is even possible due to dbplyr limitations
      warning("Multiple different schemas detected. You might need to handle these (more) manually:\n",
              paste(unique(schema), collapse = ", "))
    } else {
      schema <- unique(schema)
    }
  } else {
    stop("Could not detect object type")
  }

  if (schema == "public") schema <- "prod"

  return(schema)
}

#' Convenience function for dbplyr::in_schema
#'
#' If schema is provided but table is not provided, schema is parsed assuming a format of "schema.table".
#' @param schema Schema to look in / full specification for table "schema.table"
#' @param table Table to look for
#' @seealso [dbplyr::in_schema] which this function wraps.
#' @export
mg_in_schema <- function(schema, table = NULL) {

  # Check arguments
  checkmate::assert_character(schema)
  checkmate::assert_character(table, null.ok = TRUE)

  if (is.null(table)) { # parse arguments of the type "schema.table"
    tmp <- stringr::str_split_1(schema, "\\.")
    dbplyr::in_schema(tmp[1], tmp[2])
  } else {
    dbplyr::in_schema(schema, table)
  }
}

#' Check if table exists in db
#'
#' @template conn
#' @param db_table_name Name of the table to return or a DBI::Id specification of the table to look for.
#' @export
mg_table_exists <- function(conn, db_table_name) {

  # Check arguments
  checkmate::assert_class(conn, "DBIConnection")
  checkmate::assert(checkmate::check_character(db_table_name), checkmate::check_class(db_table_name, "Id"))

  if (inherits(db_table_name, "Id")) {
    order_vec <- c("schema", "table")

    # Force the order of names in Id to {schema}.{table}
    db_name <- attr(db_table_name, "name") %>%
      `[`(match(c("schema", "table"), names(.), nomatch = 0))
    db_schema <- purrr::pluck(db_name, "schema", .default = NA_character_)
    db_table  <- purrr::pluck(db_name, "table")
  } else if (is.character(db_table_name) && !stringr::str_detect(db_table_name, "\\.")) {
    db_schema <- NA
    db_table <- db_table_name
  } else {
    db_name <- stringr::str_split_1(db_table_name, "\\.")
    db_schema <- db_name[1]
    db_table  <- db_name[2]
  }

  n_matches <- mg_get_tables(conn) %>%
    dplyr::filter(.data$schema == db_schema, .data$table == db_table) %>%
    mg_nrow()

  return(n_matches == 1)
}

#' @import utils
utils::globalVariables("%like%")

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
  objs <- DBI::dbListObjects(conn) %>%
    dplyr::select(table)

  # purrr::map fails if .x is empty, avoid by returning early
  if (mg_nrow(objs) == 0) return(data.frame(schema = character(), table = character()))

  tables <- purrr::map(
    # For each top-level object (except tables)...
    objs$table, \(.x) {
      if (names(.x@name) == "table") {
        return(data.frame(schema = "", table = .x@name["table"]))
      }

      # ...retrieve all tables
      DBI::dbListObjects(conn, .x) %>%
        dplyr::pull(table) %>%
        purrr::map(\(.y) data.frame(schema = .x@name, table = .y@name["table"])) %>%
        purrr::reduce(rbind.data.frame)
    }) %>%
    purrr::reduce(rbind.data.frame)

  # Skip dbplyr temporary tables
  tables <- dplyr::filter(tables, !startsWith(.data$table, "dbplyr_"))

  # Skip PostgreSQL metadata tables
  if (inherits(conn, "PqConnection")) {
    tables <- dplyr::filter(tables, !(.data$schema == "information_schema" | grepl("^pg_.*", .data$schema)))
  }

  # Subset if pattern is given
  if (!is.null(pattern)) {
    tables <- subset(tables, table %like% pattern)
  }

  # Remove empty schemas
  tables <- dplyr::mutate(tables, schema = dplyr::if_else(.data$schema == "", NA, .data$schema))

  row.names(tables) <- NULL  # Reset row names
  return(tables)
}

#' Gets a named table from a given schema
#'
#' @template conn
#' @param db_table_name Name of the table to return or a DBI::Id specification of the table. If missing, a list of available tables is printed
#' @param slice_ts If set different from NA (default), the returned data looks as on the given date. If set as NULL, all data is returned
#' @param include_slice_info Default FALSE. If set TRUE, the history columns "checksum", "from_ts", "until_ts" are returned also
#' @return A "lazy" dataframe (tbl_lazy) generated using dbplyr
#' @export
mg_get_table <- function(conn, db_table_name = NULL, slice_ts = NA, include_slice_info = FALSE) {

  # Check arguments
  checkmate::assert_class(conn, "DBIConnection")
  checkmate::assert(checkmate::check_character(db_table_name, null.ok = TRUE), checkmate::check_class(db_table_name, "Id"), combine = "or")
  mg_assert_timestamp_like(slice_ts, null.ok = TRUE)
  checkmate::assert_logical(include_slice_info)

  # Get tables in db schema
  tables <- mg_get_tables(conn)

  if (is.null(db_table_name)) {

    print("Select one the following tables:")
    return(tables)

  } else {

    # Ensure existence of table
    if (!mg_table_exists(conn, db_table_name)) {
      stop(glue::glue("Table name {db_table_name} is not found!"))
    }

    # Convert to DBI::Id
    db_table_id <- mg_id(db_table_name)

    # Look-up table in DB
    q <- dplyr::tbl(conn, db_table_id)

    # Check whether data mg_is.historical
    if (mg_is.historical(q) && !is.null(slice_ts)) {

      # Filter based on date
      if (is.na(slice_ts)) {
        q <- q %>% dplyr::filter(is.na(.data$until_ts)) # Newest data
      } else {
        q <- q %>% mg_slice_time(slice_ts)
      }

      # Remove history columns
      if (!include_slice_info) {
        q <- q %>% dplyr::select(-tidyselect::any_of(c("from_ts", "until_ts", "checksum")))
      }
    }

    return(q)
  }
}

#' Read or add a comment to the database
#'
#' @template conn
#' @template db_table
#' @param column         The name of the table/column to get/set comment on (for table comment, use NA)
#' @param comment        If different from NULL (default) the comment will added to the table/column. Use a reference "@column" too look up in docs.templates. Everything after the reference gets appended.
#' @param auto_generated Indicates if comment is auto generated or supplied by the user
#' @param timestamp      Timestamp indicating when the comment is valid from
#' @return The existing comment on the column/table if no comment is provided. Else NULL
#' @importFrom rlang .data
#' @export
db_comment <- function(conn, db_table, column = NA, comment = NULL, auto_generated = FALSE, timestamp = glue::glue("{today()} 09:00:00")) {

  # Check arguments
  checkmate::assert_class(conn, "DBIConnection")
  mg_assert_dbtable_like(db_table)
  checkmate::assert_character(column)
  checkmate::assert_character(comment, null.ok = TRUE)
  checkmate::assert_logical(auto_generated)
  mg_assert_timestamp_like(timestamp)

  # Unpack the db_table specification
  if (is.character(db_table)) {
    db_table_id <- mg_in_schema(db_table)
  } else {
    db_table_id <- dbplyr::remote_name(db_table)
    if (is.null(db_table_id)) stop('The remote_name of db_table could not be determined. Try giving as a character string ("schema.table") or direct query: tbl(conn, "schema.table") instead.')
  }
  db_schema     <- purrr::pluck(db_table_id, 1) %>% as.character()
  db_table_name <- purrr::pluck(db_table_id, 2) %>% as.character()

  # Pull current comments from the DB
  if (is.na(column)) {
    existing_comment <- dplyr::tbl(conn, mg_in_schema("information_schema", "tables")) %>%
      dplyr::filter(.data$table_schema == db_schema, .data$table_name == db_table_name) %>%
      tidyr::unite("db_table", "table_schema", "table_name", sep = ".", remove = FALSE) %>%
      dplyr::transmute(schema = .data$table_schema, table = .data$table_name, column_name = NA_character_,
                       comment = dbplyr::sql('obj_description(CAST("db_table" AS "regclass"))'))
  } else {
    existing_comment <- dplyr::tbl(conn, mg_in_schema("information_schema", "columns")) %>%
      dplyr::filter(.data$table_schema == db_schema, .data$table_name == db_table_name, .data$column_name == column) %>%
      tidyr::unite("db_table", "table_schema", "table_name", sep = ".", remove = FALSE) %>%
      dplyr::transmute(schema = .data$table_schema, table = .data$table_name, .data$column_name,
                       comment = dbplyr::sql('col_description(CAST("db_table" AS "regclass"), "ordinal_position")'))
  }

  # If comment is missing, pull comment from db
  if (is.null(comment)) {
    return(existing_comment %>% dplyr::pull("comment"))

  } else { # Update the comment in the DB

    # Look for un-escaped single quotes
    comment <- stringr::str_replace_all(comment, "(?<!\')\'(?!\')", "\'\'")

    # Check for reference to docs.template
    if (comment %like% "^@") {
      tmp <- stringr::str_split_fixed(comment, " ", n = 2)
      reference <- purrr::pluck(tmp, 1) %>% gsub("^@", "", .)
      append    <- purrr::pluck(tmp, 2)

      # Find the reference in docs.templates
      reference_comment <- mg_get_table(conn, "docs.templates") %>%
        dplyr::filter(.data$column_name == reference) %>%
        dplyr::pull(comment)

      # Check reference exists
      if (length(reference_comment) == 0) stop("Reference: '", reference, "' not found in docs.templates")

      # Update the comment
      comment <- paste(reference_comment, append) %>% trimws()
    }

    if (is.na(column)) {
      DBI::dbExecute(conn, glue::glue("COMMENT ON TABLE {db_schema}.{db_table_name} IS '{comment}'"))

    } else {
      DBI::dbExecute(conn, glue::glue("COMMENT ON COLUMN {db_schema}.{db_table_name}.{column} IS '{comment}'"))
    }

    # Push to documentation DB
    target_table <- "docs.documentation"

    # Create the documentation table if it does not exist
    if (!mg_table_exists(conn, target_table)) {
      invisible(log <- capture.output(mg_update_snapshot(existing_comment %>% dplyr::mutate(auto_generated = auto_generated) %>% head(0), conn, target_table, timestamp, log_file = FALSE)))
      failed <- log[!is.na(stringr::str_extract(log, "(WARNING|ERROR)"))]
      if (length(failed) > 0) print(failed)
    }

    # Update the record in the docs database
    invisible(log <- capture.output(
      mg_update_snapshot(
        existing_comment %>% dplyr::mutate(comment = !!comment, auto_generated = !!auto_generated),
        conn, target_table, timestamp,
        filters = existing_comment %>% dplyr::select(-"comment"),
        log_file = FALSE, enforce_chronological_order = FALSE)))
    failed <- log[!is.na(stringr::str_extract(log, "(WARNING|ERROR)"))]
    if (length(failed) > 0) print(failed)
  }
}

#' Checks if lazy_query query contains historical data
#'
#' @template .data
#' @return TRUE if .data contains the columns: "checksum", "from_ts", and "until_ts". FALSE otherwise
#' @export
mg_is.historical <- function(.data) {

  # Check arguments
  mg_assert_data_like(.data)

  return(all(c("checksum", "from_ts", "until_ts") %in% colnames(.data)))
}

#' A warning to users that SQL does not match on NA by default
mg_join_warn <- function() {
  if (testthat::is_testing() || !interactive()) return()
  if (identical(parent.frame(n = 2), globalenv())) {
    rlang::warn('*_joins in database-backend does not match NA by default.\nIf your data contains NA, the columns with NA values must be supplied to "na_by", or you must specifiy na_matches = "na"',
                .frequency = "once", .frequency_id = "*_join NA warning")
  }
}

# Modified from https://stackoverflow.com/questions/48536983/how-to-concatenate-strings-of-multiple-columns-from-table-in-sql-server-using-dp




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

#' @import utils
utils::globalVariables(".")

# Get colnames from
mg_select_na_sql <- function(x, y, by, na_by, left = TRUE) {

  all_by <- c(by, na_by) # Variables to be common after join
  cxy <- dplyr::setdiff(dplyr::intersect(colnames(x), colnames(y)), all_by)   # Duplicate columns after join
  cx  <- dplyr::setdiff(colnames(x), colnames(y)) # Variables only in x
  cy  <- dplyr::setdiff(colnames(y), colnames(x)) # Variables only in y

  vars <- list(all_by, cx, cy, cxy, cxy)

  renamer <- \(suffix) suffix %>% purrr::map(~ purrr::partial(\(x, suffix) paste0(x, suffix), suffix = .))

  sql_select <- vars %>%
    purrr::map2(renamer(list(ifelse(left, ".x", ".y"), "", "", ".x", ".y")), ~ purrr::map(.x, .y)) %>%
    purrr::map(., ~ purrr::reduce(., c, .init = character(0))) %>%
    purrr::reduce(c)

  sql_names <- vars %>%
    purrr::map2(renamer(list("", "", "", ".x", ".y")), ~ purrr::map(.x, .y)) %>%
    purrr::map(., ~ purrr::reduce(., c, .init = character(0))) %>%
    purrr::reduce(c)

  names(sql_select) <- sql_names

  return(sql_select)
}

#' @export
#' @rdname joins
mg_right_join <- function(x, y, by = NULL, na_by = NULL, ...) {

  # Check arguments
  mg_assert_data_like(x)
  mg_assert_data_like(y)
  checkmate::assert_character(by, null.ok = TRUE)
  checkmate::assert_character(na_by, null.ok = TRUE)

  if (is.null(na_by)) {
    if (inherits(x, "tbl_dbi") || inherits(y, "tbl_dbi")) mg_join_warn()
    return(dplyr::right_join(x, y, by = by, ...))
  } else {
    mg_join_warn_experimental()
    sql_on <- mg_join_na_sql(by, na_by)
    renamer <- mg_select_na_sql(x, y, by, na_by, left = FALSE)
    return(dplyr::right_join(x, y, suffix = c(".x", ".y"), sql_on = sql_on, ...) %>%
             dplyr::rename(!!renamer) %>%
             dplyr::select(tidyselect::all_of(names(renamer))))
  }
}

#' SQL Joins
#'
#' @name joins
#'
#' @description Overloads the dplyr (left/right)_join to accept an na_by argument.
#' By default, joining using SQL does not match on NA / NULL.
#' dbplyr has the option "na_matches = na" to match on NA / NULL but this is very inefficient
#' This function does the matching more efficiently.
#' If a column contains NA / NULL, give the argument to na_by to match during the join
#' If no na_by is given, the function defaults to using dplyr::(left/right)_join
#' @inheritParams dplyr::left_join
#' @param na_by columns that should match on NA
#' @seealso [dplyr::mutate-joins] which this function wraps.
#' @seealso [dbplyr::join.tbl_sql] which this function wraps.
#' @export
mg_left_join <- function(x, y, by = NULL, na_by = NULL, ...) {

  # Check arguments
  mg_assert_data_like(x)
  mg_assert_data_like(y)
  checkmate::assert_character(by, null.ok = TRUE)
  checkmate::assert_character(na_by, null.ok = TRUE)

  if (is.null(na_by)) {
    if (inherits(x, "tbl_dbi") || inherits(y, "tbl_dbi")) mg_join_warn()
    return(dplyr::left_join(x, y, by = by, ...))
  } else {
    mg_join_warn_experimental()
    sql_on <- mg_join_na_sql(by, na_by)
    renamer <- mg_select_na_sql(x, y, by, na_by)
    return(dplyr::left_join(x, y, suffix = c(".x", ".y"), sql_on = sql_on, ...) %>%
             dplyr::rename(!!renamer) %>%
             dplyr::select(tidyselect::all_of(names(renamer))))


  }
}

#' @export
#' @rdname joins
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
    return(dplyr::inner_join(x, y, suffix = c(".x", ".y"), sql_on = sql_on, ...) %>%
             dplyr::rename(!!renamer) %>%
             dplyr::select(tidyselect::all_of(names(renamer))))
  }
}

#' @import utils
utils::globalVariables("valid_from")

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
  from_cols <- seq_along(tables) %>%
    purrr::map_chr(\(t) paste0("t", t, ".from")) %>%
    purrr::map_chr(\(col) ifelse(col %in% names(colnames), colnames[col], "valid_from"))

  until_cols <- seq_along(tables) %>%
    purrr::map_chr(\(t) paste0("t", t, ".until")) %>%
    purrr::map_chr(\(col) ifelse(col %in% names(colnames), colnames[col], "valid_until"))


  # Rename valid_from / valid_until columns
  tables <- tables %>%
    purrr::map2(from_cols,  \(table, from_col)  table %>% dplyr::rename(valid_from  = !!from_col)) %>%
    purrr::map2(until_cols, \(table, until_col) table %>% dplyr::rename(valid_until = !!until_col))


  # Get all changes to valid_from / valid_until
  q1 <- tables %>% purrr::map(\(table) table %>% dplyr::select(tidyselect::all_of(by), "valid_from"))
  q2 <- tables %>% purrr::map(\(table) table %>% dplyr::select(tidyselect::all_of(by), "valid_until") %>% dplyr::rename(valid_from = "valid_until"))
  t <- union(q1, q2) %>% purrr::reduce(union)

  # Sort and find valid_until in the combined validities
  t <- t %>%
    dplyr::group_by(dplyr::across(tidyselect::all_of(by))) %>%
    dbplyr::window_order(valid_from) %>%
    dplyr::mutate(.row_number_id = dplyr::row_number())

  t <- dplyr::left_join(t %>%
                          dplyr::filter(.data$.row_number_id < dplyr::n()),
                        t %>%
                          dplyr::filter(.data$.row_number_id > 1) %>%
                          dplyr::mutate(.row_number_id = .data$.row_number_id - 1) %>%
                          dplyr::rename(valid_until = valid_from),
                        by = c(by, ".row_number_id")) %>%
    dplyr::select(-".row_number_id") %>%
    dplyr::ungroup()


  # Merge data onto the new validities using non-equi joins
  joiner <- \(.data, table) dplyr::left_join(.data, table,
                                             suffix = c("", ".tmp"),
                                             sql_on = paste0(
                                               '"LHS"."', by, '" = "RHS"."', by, '" AND
                                                "LHS"."valid_from"   >= "RHS"."valid_from" AND
                                               ("LHS"."valid_until" <= "RHS"."valid_until" OR "RHS"."valid_until" IS NULL)')) %>%
    dplyr::select(-tidyselect::ends_with(".tmp")) %>%
    dplyr::relocate(tidyselect::starts_with("valid_"), .after = tidyselect::everything())

  return(purrr::reduce(tables, joiner, .init = t))
}

#' @import utils
utils::globalVariables(c("COALESCE", "TO_TIMESTAMP"))

#' Update a historical table
#' @template .data_dbi
#' @template conn
#' @template db_table
#' @param timestamp   A timestamp (POSIXct) with which to update from_ts/until_ts columns
#' @template filters
#' @param message     A message to add to the log-file (useful for supplying metadata to the log)
#' @param tic         A timestamp when computation began. If not supplied, it will be created at call-time. (Used to more accurately convey how long runtime of the update process has been)
#' @param log_file    A logical that controls whether or not to produce a log file (useful for debugging). TRUE: log file is produced and copied to DB. FALSE: no log file and no updates to DB. NULL: no logfile but DB is updated
#' @param log_db      The log table in the DB where logging information should be stored (default prod.logs)
#' @param enforce_chronological_order A logical that controls whether or not to check if timestamp of update is prior to timstamps in the DB
#' @return NULL
#' @seealso mg_filter_keys
#' @importFrom rlang .data
#' @export
mg_update_snapshot <- function(.data, conn, db_table, timestamp, filters = NULL, message = NULL, tic = Sys.time(), log_file = TRUE, log_db = "prod.logs", enforce_chronological_order = TRUE) {

  # Check arguments
  checkmate::assert_class(.data, "tbl_dbi")
  checkmate::assert_class(conn, "DBIConnection")
  mg_assert_dbtable_like(db_table)
  mg_assert_timestamp_like(timestamp)
  checkmate::assert_class(filters, "tbl_dbi", null.ok = TRUE)
  checkmate::assert_character(message, null.ok = TRUE)
  mg_assert_timestamp_like(tic)
  checkmate::assert_logical(log_file, null.ok = TRUE)
  checkmate::assert_character(log_db, pattern = r"{\w+.\w+}")
  checkmate::assert_logical(enforce_chronological_order)


  # If db_table is given as str, fetch the actual table
  # If the table does not exist, create an empty table using the signature of the incoming data
  if (is.character(db_table)) {

    db_table_name <- db_table
    db_table_id <- mg_in_schema(db_table)

    if (mg_table_exists(conn, db_table)) {
      db_table <- dplyr::tbl(conn, db_table_id)
    } else {
      db_table_signature <- .data %>%
        dplyr::mutate("checksum" = as.character(NA),
                      "from_ts"  = DBI::SQL("NOW()::timestamp"), # Cast as timestamp without timezone
                      "until_ts" = DBI::SQL("NOW()::timestamp")) %>%
        utils::head(0)
      db_table <- dplyr::copy_to(conn, db_table_signature, db_table_id, temporary = FALSE)
      DBI::dbExecute(conn, glue::glue("ALTER TABLE {db_table_name} ADD PRIMARY KEY (checksum, from_ts)"))
    }
  } else {
    db_table_id <- dbplyr::remote_name(db_table)
    db_table_name <- db_table_id %>% as.character() %>% stringr::str_remove_all('\"')
  }

  # Determine how logging should be done
  # if log_file = TRUE, we write log and to DB
  # if log_file = NULL, we write only to DB
  # if log_file = FALSE, we do not write any logging
  if (is.null(log_file) || log_file == FALSE) {

    logger_info <- logger_warn <- logger_error  <- function(...) invisible(NULL)

  } else {

    log_file <- paste0("/ngc/projects/ssi_mg/logs/",
                       format(tic, "%Y%m%d.%H%M."),
                       format(as.Date(timestamp), "%Y_%m_%d_"),
                       db_table_name, ".log")

    # Prevent appending to existing log file
    if (file.exists(log_file)) {
      stop("Log file already exists at given timestamp.")
    }

    # Create file loggers
    logger_info  <- purrr::partial(mg_log_info,  log_file = log_file)
    logger_warn  <- purrr::partial(mg_log_warn,  log_file = log_file)
    logger_error <- purrr::partial(mg_log_error, log_file = log_file)
  }

  if (!is.null(log_file) && log_file == FALSE) {

    logger_db <- function(...) invisible(NULL)

  } else {

    # If log_file is NULL (ie. no file should be generated), we use a generate a "fake" log_file variable to join by
    # In principle, we should rewrite to handle these cases more elegantly
    if (is.null(log_file)) log_file <- tempfile(tmpdir = "", pattern = "")

    # Add line to DB
    dplyr::rows_append(x = dplyr::tbl(conn, mg_in_schema(log_db)),
                       y = data.frame(log_file = basename(log_file)),
                       copy = TRUE, in_place = TRUE)
    logger_db <- function(...) {
      dplyr::rows_patch(x = dplyr::tbl(conn, mg_in_schema(log_db)),
                        y = data.frame(..., log_file = basename(log_file)),
                        by = "log_file", copy = TRUE, in_place = TRUE, unmatched = "ignore")
    }

  }


  logger_db(start_time = tic)
  logger_info("Started", tic = tic) # Use input time in log

  # Add message to log (if given)
  if (!is.null(message)) {
    logger_db(message = message)
    logger_info("Message:", message, tic = tic)
  }


  # Opening checks
  if (!mg_is.historical(db_table)) {
    logger_db(success = FALSE, end_time = tic)
    logger_error("Table does not seem like a historical table", tic = tic) # Use input time in log
  }

  if (!setequal(colnames(.data), colnames(
    dplyr::select(db_table, -c("checksum", "from_ts", "until_ts")))
  )) {
    logger_db(success = FALSE, end_time = tic)
    logger_error("Columns do not match!\n",
                 "Table columns:\n",
                 paste(colnames(db_table %>% dplyr::select(-tidyselect::any_of(c("checksum", "from_ts", "until_ts")))), collapse = ", "),
                  "\nInput columns:\n",
                 paste(colnames(.data), collapse = ", "), tic = tic) # Use input time in log
  }

  db_schema <- purrr::pluck(as.character(db_table_id), 1)
  logger_db(schema = ifelse(db_schema == "prod", NA, db_schema), table = purrr::pluck(as.character(db_table_id), 2))
  logger_info("Parsing data for table", db_table_name, "started", tic = tic) # Use input time in log
  logger_db(date = as.POSIXlt(timestamp, tz = "Europe/Copenhagen"))
  logger_info("Given timestamp for table is", timestamp, tic = tic) # Use input time in log

  # Check for current update status
  db_latest <- db_table %>%
    dplyr::summarize(COALESCE(max(.data$from_ts, na.rm = TRUE), TO_TIMESTAMP("1900-01-01 00:00:00", "YYYY-MM-DD HH24:MI::SS"))) %>%
    dplyr::pull()


  if (enforce_chronological_order && as.POSIXct(timestamp, tz = "Europe/Copenhagen") < db_latest) {
    logger_db(success = FALSE, end_time = tic)
    logger_error("Given timestamp", timestamp, "is earlier than latest",
              "timestamp in table:", db_latest, tic = tic) # Use input time in log
  }

  # Compute .data immediately to reduce runtime and compute checksum
  .data <- .data %>%
    dplyr::ungroup() %>%
    dplyr::select(
      colnames(dplyr::select(db_table, -tidyselect::any_of(c("checksum", "from_ts", "until_ts"))))
    ) %>%
    mg_digest_to_checksum(col = "checksum", warn = TRUE) %>%
    mg_filter_keys(filters) %>%
    dplyr::compute()


  # Apply filter to current records
  db_table <- mg_filter_keys(db_table, filters)

  # Determine the next timestamp in the data (can be NA if none is found)
  next_timestamp <- min(
    db_table %>%
      dplyr::filter(.data$from_ts  > timestamp) %>%
      dplyr::summarize(next_timestamp = min(.data$from_ts, na.rm = TRUE)) %>%
      dplyr::pull("next_timestamp"),
    db_table %>%
      dplyr::filter(.data$until_ts > timestamp) %>%
      dplyr::summarize(next_timestamp = min(.data$until_ts, na.rm = TRUE)) %>%
      dplyr::pull("next_timestamp")) %>%
    format("%Y-%m-%d %H:%M:%S")

  # Consider only records valid at timestamp (and apply the filter if present)
  db_table <- mg_slice_time(db_table, timestamp)


  # Count open rows at timestamp
  nrow_open <- mg_nrow(db_table)


  # Select only data with no until_ts and with different values in any fields
  logger_info("Deactivating records")
  if (nrow_open > 0) {
    to_remove <- dplyr::setdiff(dplyr::select(db_table, colnames(.data)), .data) %>%
      dplyr::compute() # Something has changed in dbplyr (2.2.1) that makes this compute needed. Code that takes 20 secs with can be more than 30 minutes to compute without...

    nrow_to_remove <- mg_nrow(to_remove)

    to_remove <- to_remove %>%
      dplyr::left_join(dplyr::select(db_table, "from_ts", "checksum"), by = "checksum") %>%
      dplyr::transmute(.data$checksum, .data$from_ts, until_ts = TO_TIMESTAMP(timestamp, "YYYY-MM-DD HH24:MI::SS"))
  } else {
    nrow_to_remove <- 0
  }
  logger_info("After to_remove")

  to_add <- dplyr::setdiff(.data, dplyr::select(db_table, colnames(.data))) %>%
    dplyr::mutate(from_ts  = TO_TIMESTAMP(timestamp,      "YYYY-MM-DD HH24:MI::SS"),
                  until_ts = TO_TIMESTAMP(next_timestamp, "YYYY-MM-DD HH24:MI::SS")) %>% # Instead of NA, use largest timestamp larger than given timestamp
    dplyr::compute()

  nrow_to_add <- mg_nrow(to_add)
  logger_info("After to_add")


  if (nrow_to_remove > 0) {
    dplyr::rows_update(x = dplyr::tbl(conn, db_table_id),
                       y = to_remove,
                       by = c("checksum", "from_ts"),
                       in_place = TRUE,
                       unmatched = "ignore")
  }

  #nrow_cum_removed <- dplyr::tbl(conn, db_table_id) %>% dplyr::filter(.data$until_ts == timestamp, .data$from_ts != .data$until_ts) %>% mg_nrow()
  logger_db(n_deactivations = nrow_to_remove) # Logs table contains the aggregate number of added records on the day
  logger_info("Deactivate records count:", nrow_to_remove)
  logger_info("Adding new records")

  if (nrow_to_add > 0) {
    dplyr::rows_insert(x = dplyr::tbl(conn, db_table_id),
                       y = to_add,
                       by = c("checksum", "from_ts"),
                       in_place = TRUE,
                       conflict = "ignore")
  }

  #nrow_cum_added <- dplyr::tbl(conn, db_table_id) %>% dplyr::filter(.data$from_ts == timestamp, is.na(.data$until_ts) || .data$from_ts != .data$until_ts) %>% mg_nrow()
  logger_db(n_insertions = nrow_to_add)
  logger_info("Insert records count:", nrow_to_add)


  # If several updates come in a single day, some records may have from_ts = until_ts.
  # We remove these records here
  redundant_rows <- dplyr::tbl(conn, db_table_id) %>%
    dplyr::filter(.data$from_ts == .data$until_ts) %>%
    dplyr::select("checksum", "from_ts")
  nrow_redundant <- mg_nrow(redundant_rows)

  if (nrow_redundant > 0) {
    dplyr::rows_delete(
      dplyr::tbl(conn, db_table_id),
      redundant_rows,
      by = c("checksum", "from_ts"),
      in_place = TRUE, unmatched = "ignore")
    logger_info("Doubly updated records removed:", nrow_redundant)
  }

  # If chronological order is not enforced, some records may be split across several records
  # checksum is the same, and from_ts / until_ts are continuous
  # We collapse these records here
  if (!enforce_chronological_order) {
    redundant_rows <- dplyr::tbl(conn, db_table_id) %>%
      mg_filter_keys(filters)

    redundant_rows <- dplyr::inner_join(
        redundant_rows,
        redundant_rows %>% dplyr::select("checksum", "from_ts", "until_ts"),
        suffix = c("", ".p"),
        sql_on = '"RHS"."checksum" = "LHS"."checksum" AND "LHS"."until_ts" = "RHS"."from_ts"'
      ) %>%
      dplyr::select(-"checksum.p")

    redundant_rows_to_delete <- redundant_rows %>%
      dplyr::transmute(.data$checksum, from_ts = .data$from_ts.p) %>%
      dplyr::compute()

    redundant_rows_to_update <- redundant_rows %>%
      dplyr::transmute(.data$checksum, from_ts = .data$from_ts, until_ts = .data$until_ts.p) %>%
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
      logger_info("Continous records collapsed:", mg_nrow(redundant_rows_to_update))
    }

  }

  toc <- Sys.time()
  logger_db(end_time = toc, duration = format(round(difftime(toc, tic), digits = 2)), success = TRUE)
  logger_info("Finished processing for table", db_table_name, tic = toc)

}

#' @import utils
utils::globalVariables("MD5")

#' Computes an MD5 checksum from columns
#'
#' @template .data_dbi
#' @param col Name of the column to put the checksums in
#' @param warn Flag to warn if target column already exists in data
#'
#' @importFrom rlang `:=`
#' @export
mg_digest_to_checksum <- function(.data, col = "checksum", warn = TRUE) {

  # Check arguments
  checkmate::assert_class(.data, "tbl_PqConnection")
  checkmate::assert_character(col)
  checkmate::assert_logical(warn)

  if (as.character(dplyr::ensym(col)) %in% colnames(.data) && warn) {
    warning("Column ",
            as.character(dplyr::ensym(col)),
            " already exists in data and will be overwritten!")
  }

  colnames <- .data %>%
    dplyr::select(-tidyselect::any_of(c("from_ts", "until_ts", "checksum"))) %>%
    colnames()

  colname_symbols <- purrr::map(paste0(colnames, ".chr"),
                                as.symbol)

  out <- .data %>%
    dplyr::mutate(dplyr::across(tidyselect::all_of(colnames), paste, .names = "{.col}.chr")) %>%
    dplyr::mutate({{ col }} := MD5(paste(!!!colname_symbols, sep = "_"))) %>%
    dplyr::select(tidyselect::all_of(c(colnames(.data), col)))

  return(out)
}

#' Provides age_labels that follows the mg standard
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

#' checkmate helper: Check for "generic" timestamp type
#' @param timestamp Object to test if is POSIX or character
#' @param ...       parameters passed to checkmate::check_*
#' @export
mg_assert_timestamp_like <- function(timestamp, ...) {
  checkmate::assert(
    checkmate::check_posixct(timestamp, ...),
    checkmate::check_character(timestamp, ...),
    checkmate::check_date(timestamp, ...)
  )
}

#' checkmate helper: Check for "generic" db_table type
#' @param db_table Object to test if is of class "tbl_dbi" or character on form "schema.table"
#' @param ...       parameters passed to checkmate::check_*
#' @export
mg_assert_dbtable_like <- function(db_table, ...) {
  checkmate::assert(
    checkmate::check_character(db_table, pattern = r"{^\w*.\w*$}", ...),
    checkmate::check_class(db_table, "tbl_dbi", ...)
  )
}

#' checkmate helper: Check for "generic" data.table/data.frame/tbl/tibble type
#' @param .data Object to test if is data.table, data.frame, tbl or tibble
#' @param ...   parameters passed to checkmate::check_*
#' @export
mg_assert_data_like <- function(.data, ...) {
  checkmate::assert(
    checkmate::check_class(.data, "tbl_dbi", ...),
    checkmate::check_data_frame(.data, ...),
    checkmate::check_data_table(.data, ...),
    checkmate::check_tibble(.data, ...)
  )
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
      key_types <- filters %>%
        dplyr::ungroup() %>%
        dplyr::summarise(dplyr::across(.fns = ~ any(is.na(.), na.rm = TRUE))) %>%
        tidyr::pivot_longer(tidyselect::everything(), names_to = "column_name", values_to = "is_na")

      by    <- key_types %>% dplyr::filter(!.data$is_na) %>% dplyr::pull("column_name")
      na_by <- key_types %>% dplyr::filter( .data$is_na) %>% dplyr::pull("column_name")

      if (length(by) == 0)    by    <- NULL
      if (length(na_by) == 0) na_by <- NULL
    }
    return(mg_inner_join(.data, filters, by = by, na_by = na_by))
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

#' @rdname log_printing
#' @export
mg_log_error <- function(..., log_type = "ERROR") {
  mg_log_info(..., log_type = log_type)
  stop()
}

#' @rdname log_printing
#' @export
mg_log_warn <- function(..., log_type = "WARNING") {
  mg_log_info(..., log_type = log_type)
  warning()
}

#' Convenience function to produce log output
#'
#' @description
#' mg_log_error also calls stop(...). mg_log_warn also calls warning(...).
#'
#' @name log_printing
#' @param ...        Arguments passed to paste (to produce log message)
#' @param tic        The timestamp used by the log entry (default Sys.time())
#' @param log_file   Path to log-file. If not set, log entries are printed to console
#' @param log_type   The type of information given to log (written before message in the log)
#' @export
mg_log_info <- function(..., tic = Sys.time(), log_file = "/dev/null", log_type = "INFO") {
  mg_printr(paste(mg_format_timestamp(tic), Sys.getenv("USER"), log_type, paste(...), sep = " - "), file = log_file)
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
  .data <- .data %>%
    dplyr::filter(is.na({{until_ts}}) | slice_ts < {{until_ts}},
                  {{from_ts}} <= slice_ts)
  return(.data)
}

#' cat printing with default new line
#'
#' @param ...  The normal input to cat
#' @param file Path of an output file to append the output to
#' @param sep  The separator given to cat
#' @export
mg_printr <- function(..., file = "/dev/null", sep = "") {
  sink(file = file, split = TRUE, append = TRUE, type = "output")
  cat(..., "\n", sep = sep)
  sink()
}

#' formats timestamps as 2022-13-09 09:30,213
#'
#' @param ts  Timestamp to format
#' @export
mg_format_timestamp <- function(ts) {
  return(stringr::str_replace(format(ts, "%F %H:%M:%OS3", locale = "en"), "\\.", ","))
}

#' Gets a named table from a given schema
#'
#' @template conn
#' @param db_table_name Name of the table to return or a DBI::Id specification of the table. If missing, a list of available tables is printed
#' @param slice_ts If set different from NA (default), the returned data looks as on the given date. If set as NULL, all data is returned
#' @param include_slice_info Default FALSE. If set TRUE, the history columns "checksum", "from_ts", "until_ts" are returned also
#' @return A "lazy" dataframe (tbl_lazy) generated using dbplyr
#' @export
mg_get_table <- function(conn, db_table_name = NULL, slice_ts = NA, include_slice_info = FALSE) {

  # Check arguments
  checkmate::assert_class(conn, "DBIConnection")
  checkmate::assert(checkmate::check_character(db_table_name, null.ok = TRUE), checkmate::check_class(db_table_name, "Id"), combine = "or")
  mg_assert_timestamp_like(slice_ts, null.ok = TRUE)
  checkmate::assert_logical(include_slice_info)

  # Get tables in db schema
  tables <- mg_get_tables(conn)

  if (is.null(db_table_name)) {

    print("Select one the following tables:")
    return(tables)

  } else {

    # Ensure existence of table
    if (!mg_table_exists(conn, db_table_name)) {
      stop(glue::glue("Table name {db_table_name} is not found!"))
    }

    # Convert to DBI::Id
    db_table_id <- mg_id(db_table_name)

    # Look-up table in DB
    q <- dplyr::tbl(conn, db_table_id)

    # Check whether data mg_is.historical
    if (mg_is.historical(q) && !is.null(slice_ts)) {

      # Filter based on date
      if (is.na(slice_ts)) {
        q <- q %>% dplyr::filter(is.na(.data$until_ts)) # Newest data
      } else {
        q <- q %>% mg_slice_time(slice_ts)
      }

      # Remove history columns
      if (!include_slice_info) {
        q <- q %>% dplyr::select(-tidyselect::any_of(c("from_ts", "until_ts", "checksum")))
      }
    }

    return(q)
  }
}

#' Convenience function for DBI::Id
#'
#' @param db_table_name Name of the table to return. Can implicitly contain information about schema.
#' @seealso [DBI::Id] which this function wraps.
#' @export
mg_id <- function(db_table_name) {

  # Check if already Id
  if (inherits(db_table_name, "Id")) return(db_table_name)

  # Check arguments
  checkmate::assert_character(db_table_name)

  if (is.character(db_table_name) && !stringr::str_detect(db_table_name, "\\.")) {
    return(DBI::Id(table = db_table_name))
  } else {
    db_name <- stringr::str_split_1(db_table_name, "\\.")
    db_schema <- db_name[1]
    db_table  <- db_name[2]
    return(DBI::Id(schema = db_schema, table = db_table))
  }
}

#' @import utils
utils::globalVariables(c("COALESCE", "TO_TIMESTAMP"))

#' Update a historical table
#' @template .data_dbi
#' @template conn
#' @template db_table
#' @param timestamp   A timestamp (POSIXct) with which to update from_ts/until_ts columns
#' @template filters
#' @param message     A message to add to the log-file (useful for supplying metadata to the log)
#' @param tic         A timestamp when computation began. If not supplied, it will be created at call-time. (Used to more accurately convey how long runtime of the update process has been)
#' @param log_file    A logical that controls whether or not to produce a log file (useful for debugging). TRUE: log file is produced and copied to DB. FALSE: no log file and no updates to DB. NULL: no logfile but DB is updated
#' @param log_db      The log table in the DB where logging information should be stored (default prod.logs)
#' @param enforce_chronological_order A logical that controls whether or not to check if timestamp of update is prior to timstamps in the DB
#' @return NULL
#' @seealso mg_filter_keys
#' @importFrom rlang .data
#' @export
mg_update_snapshot <- function(.data, conn, db_table, timestamp, filters = NULL, message = NULL, tic = Sys.time(), log_file = TRUE, log_db = "prod.logs", enforce_chronological_order = TRUE) {

  # Check arguments
  checkmate::assert_class(.data, "tbl_dbi")
  checkmate::assert_class(conn, "DBIConnection")
  mg_assert_dbtable_like(db_table)
  mg_assert_timestamp_like(timestamp)
  checkmate::assert_class(filters, "tbl_dbi", null.ok = TRUE)
  checkmate::assert_character(message, null.ok = TRUE)
  mg_assert_timestamp_like(tic)
  checkmate::assert_logical(log_file, null.ok = TRUE)
  checkmate::assert_character(log_db, pattern = r"{\w+.\w+}")
  checkmate::assert_logical(enforce_chronological_order)


  # If db_table is given as str, fetch the actual table
  # If the table does not exist, create an empty table using the signature of the incoming data
  if (is.character(db_table)) {

    db_table_name <- db_table
    db_table_id <- mg_in_schema(db_table)

    if (mg_table_exists(conn, db_table)) {
      db_table <- dplyr::tbl(conn, db_table_id)
    } else {
      db_table_signature <- .data %>%
        dplyr::mutate("checksum" = as.character(NA),
                      "from_ts"  = DBI::SQL("NOW()::timestamp"), # Cast as timestamp without timezone
                      "until_ts" = DBI::SQL("NOW()::timestamp")) %>%
        utils::head(0)
      db_table <- dplyr::copy_to(conn, db_table_signature, db_table_id, temporary = FALSE)
      DBI::dbExecute(conn, glue::glue("ALTER TABLE {db_table_name} ADD PRIMARY KEY (checksum, from_ts)"))
    }
  } else {
    db_table_id <- dbplyr::remote_name(db_table)
    db_table_name <- db_table_id %>% as.character() %>% stringr::str_remove_all('\"')
  }

  # Determine how logging should be done
  # if log_file = TRUE, we write log and to DB
  # if log_file = NULL, we write only to DB
  # if log_file = FALSE, we do not write any logging
  if (is.null(log_file) || log_file == FALSE) {

    logger_info <- logger_warn <- logger_error  <- function(...) invisible(NULL)

  } else {

    log_file <- paste0("/ngc/projects/ssi_mg/logs/",
                       format(tic, "%Y%m%d.%H%M."),
                       format(as.Date(timestamp), "%Y_%m_%d_"),
                       db_table_name, ".log")

    # Prevent appending to existing log file
    if (file.exists(log_file)) {
      stop("Log file already exists at given timestamp.")
    }

    # Create file loggers
    logger_info  <- purrr::partial(mg_log_info,  log_file = log_file)
    logger_warn  <- purrr::partial(mg_log_warn,  log_file = log_file)
    logger_error <- purrr::partial(mg_log_error, log_file = log_file)
  }

  if (!is.null(log_file) && log_file == FALSE) {

    logger_db <- function(...) invisible(NULL)

  } else {

    # If log_file is NULL (ie. no file should be generated), we use a generate a "fake" log_file variable to join by
    # In principle, we should rewrite to handle these cases more elegantly
    if (is.null(log_file)) log_file <- tempfile(tmpdir = "", pattern = "")

    # Add line to DB
    dplyr::rows_append(x = dplyr::tbl(conn, mg_in_schema(log_db)),
                       y = data.frame(log_file = basename(log_file)),
                       copy = TRUE, in_place = TRUE)
    logger_db <- function(...) {
      dplyr::rows_patch(x = dplyr::tbl(conn, mg_in_schema(log_db)),
                        y = data.frame(..., log_file = basename(log_file)),
                        by = "log_file", copy = TRUE, in_place = TRUE, unmatched = "ignore")
    }

  }


  logger_db(start_time = tic)
  logger_info("Started", tic = tic) # Use input time in log

  # Add message to log (if given)
  if (!is.null(message)) {
    logger_db(message = message)
    logger_info("Message:", message, tic = tic)
  }


  # Opening checks
  if (!mg_is.historical(db_table)) {
    logger_db(success = FALSE, end_time = tic)
    logger_error("Table does not seem like a historical table", tic = tic) # Use input time in log
  }

  if (!setequal(colnames(.data), colnames(
    dplyr::select(db_table, -c("checksum", "from_ts", "until_ts")))
  )) {
    logger_db(success = FALSE, end_time = tic)
    logger_error("Columns do not match!\n",
                 "Table columns:\n",
                 paste(colnames(db_table %>% dplyr::select(-tidyselect::any_of(c("checksum", "from_ts", "until_ts")))), collapse = ", "),
                  "\nInput columns:\n",
                 paste(colnames(.data), collapse = ", "), tic = tic) # Use input time in log
  }

  db_schema <- purrr::pluck(as.character(db_table_id), 1)
  logger_db(schema = ifelse(db_schema == "prod", NA, db_schema), table = purrr::pluck(as.character(db_table_id), 2))
  logger_info("Parsing data for table", db_table_name, "started", tic = tic) # Use input time in log
  logger_db(date = as.POSIXlt(timestamp, tz = "Europe/Copenhagen"))
  logger_info("Given timestamp for table is", timestamp, tic = tic) # Use input time in log

  # Check for current update status
  db_latest <- db_table %>%
    dplyr::summarize(COALESCE(max(.data$from_ts, na.rm = TRUE), TO_TIMESTAMP("1900-01-01 00:00:00", "YYYY-MM-DD HH24:MI::SS"))) %>%
    dplyr::pull()


  if (enforce_chronological_order && as.POSIXct(timestamp, tz = "Europe/Copenhagen") < db_latest) {
    logger_db(success = FALSE, end_time = tic)
    logger_error("Given timestamp", timestamp, "is earlier than latest",
              "timestamp in table:", db_latest, tic = tic) # Use input time in log
  }

  # Compute .data immediately to reduce runtime and compute checksum
  .data <- .data %>%
    dplyr::ungroup() %>%
    dplyr::select(
      colnames(dplyr::select(db_table, -tidyselect::any_of(c("checksum", "from_ts", "until_ts"))))
    ) %>%
    mg_digest_to_checksum(col = "checksum", warn = TRUE) %>%
    mg_filter_keys(filters) %>%
    dplyr::compute()


  # Apply filter to current records
  db_table <- mg_filter_keys(db_table, filters)

  # Determine the next timestamp in the data (can be NA if none is found)
  next_timestamp <- min(
    db_table %>%
      dplyr::filter(.data$from_ts  > timestamp) %>%
      dplyr::summarize(next_timestamp = min(.data$from_ts, na.rm = TRUE)) %>%
      dplyr::pull("next_timestamp"),
    db_table %>%
      dplyr::filter(.data$until_ts > timestamp) %>%
      dplyr::summarize(next_timestamp = min(.data$until_ts, na.rm = TRUE)) %>%
      dplyr::pull("next_timestamp")) %>%
    format("%Y-%m-%d %H:%M:%S")

  # Consider only records valid at timestamp (and apply the filter if present)
  db_table <- mg_slice_time(db_table, timestamp)


  # Count open rows at timestamp
  nrow_open <- mg_nrow(db_table)


  # Select only data with no until_ts and with different values in any fields
  logger_info("Deactivating records")
  if (nrow_open > 0) {
    to_remove <- dplyr::setdiff(dplyr::select(db_table, colnames(.data)), .data) %>%
      dplyr::compute() # Something has changed in dbplyr (2.2.1) that makes this compute needed. Code that takes 20 secs with can be more than 30 minutes to compute without...

    nrow_to_remove <- mg_nrow(to_remove)

    to_remove <- to_remove %>%
      dplyr::left_join(dplyr::select(db_table, "from_ts", "checksum"), by = "checksum") %>%
      dplyr::transmute(.data$checksum, .data$from_ts, until_ts = TO_TIMESTAMP(timestamp, "YYYY-MM-DD HH24:MI::SS"))
  } else {
    nrow_to_remove <- 0
  }
  logger_info("After to_remove")

  to_add <- dplyr::setdiff(.data, dplyr::select(db_table, colnames(.data))) %>%
    dplyr::mutate(from_ts  = TO_TIMESTAMP(timestamp,      "YYYY-MM-DD HH24:MI::SS"),
                  until_ts = TO_TIMESTAMP(next_timestamp, "YYYY-MM-DD HH24:MI::SS")) %>% # Instead of NA, use largest timestamp larger than given timestamp
    dplyr::compute()

  nrow_to_add <- mg_nrow(to_add)
  logger_info("After to_add")


  if (nrow_to_remove > 0) {
    dplyr::rows_update(x = dplyr::tbl(conn, db_table_id),
                       y = to_remove,
                       by = c("checksum", "from_ts"),
                       in_place = TRUE,
                       unmatched = "ignore")
  }

  #nrow_cum_removed <- dplyr::tbl(conn, db_table_id) %>% dplyr::filter(.data$until_ts == timestamp, .data$from_ts != .data$until_ts) %>% mg_nrow()
  logger_db(n_deactivations = nrow_to_remove) # Logs table contains the aggregate number of added records on the day
  logger_info("Deactivate records count:", nrow_to_remove)
  logger_info("Adding new records")

  if (nrow_to_add > 0) {
    dplyr::rows_insert(x = dplyr::tbl(conn, db_table_id),
                       y = to_add,
                       by = c("checksum", "from_ts"),
                       in_place = TRUE,
                       conflict = "ignore")
  }

  #nrow_cum_added <- dplyr::tbl(conn, db_table_id) %>% dplyr::filter(.data$from_ts == timestamp, is.na(.data$until_ts) || .data$from_ts != .data$until_ts) %>% mg_nrow()
  logger_db(n_insertions = nrow_to_add)
  logger_info("Insert records count:", nrow_to_add)


  # If several updates come in a single day, some records may have from_ts = until_ts.
  # We remove these records here
  redundant_rows <- dplyr::tbl(conn, db_table_id) %>%
    dplyr::filter(.data$from_ts == .data$until_ts) %>%
    dplyr::select("checksum", "from_ts")
  nrow_redundant <- mg_nrow(redundant_rows)

  if (nrow_redundant > 0) {
    dplyr::rows_delete(
      dplyr::tbl(conn, db_table_id),
      redundant_rows,
      by = c("checksum", "from_ts"),
      in_place = TRUE, unmatched = "ignore")
    logger_info("Doubly updated records removed:", nrow_redundant)
  }

  # If chronological order is not enforced, some records may be split across several records
  # checksum is the same, and from_ts / until_ts are continuous
  # We collapse these records here
  if (!enforce_chronological_order) {
    redundant_rows <- dplyr::tbl(conn, db_table_id) %>%
      mg_filter_keys(filters)

    redundant_rows <- dplyr::inner_join(
        redundant_rows,
        redundant_rows %>% dplyr::select("checksum", "from_ts", "until_ts"),
        suffix = c("", ".p"),
        sql_on = '"RHS"."checksum" = "LHS"."checksum" AND "LHS"."until_ts" = "RHS"."from_ts"'
      ) %>%
      dplyr::select(-"checksum.p")

    redundant_rows_to_delete <- redundant_rows %>%
      dplyr::transmute(.data$checksum, from_ts = .data$from_ts.p) %>%
      dplyr::compute()

    redundant_rows_to_update <- redundant_rows %>%
      dplyr::transmute(.data$checksum, from_ts = .data$from_ts, until_ts = .data$until_ts.p) %>%
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
      logger_info("Continous records collapsed:", mg_nrow(redundant_rows_to_update))
    }

  }

  toc <- Sys.time()
  logger_db(end_time = toc, duration = format(round(difftime(toc, tic), digits = 2)), success = TRUE)
  logger_info("Finished processing for table", db_table_name, tic = toc)

}

#' Read or add a comment to the database
#'
#' @template conn
#' @template db_table
#' @param column         The name of the table/column to get/set comment on (for table comment, use NA)
#' @param comment        If different from NULL (default) the comment will added to the table/column. Use a reference "@column" too look up in docs.templates. Everything after the reference gets appended.
#' @param auto_generated Indicates if comment is auto generated or supplied by the user
#' @param timestamp      Timestamp indicating when the comment is valid from
#' @return The existing comment on the column/table if no comment is provided. Else NULL
#' @importFrom rlang .data
#' @export
db_comment <- function(conn, db_table, column = NA, comment = NULL, auto_generated = FALSE, timestamp = glue::glue("{today()} 09:00:00")) {

  # Check arguments
  checkmate::assert_class(conn, "DBIConnection")
  mg_assert_dbtable_like(db_table)
  checkmate::assert_character(column)
  checkmate::assert_character(comment, null.ok = TRUE)
  checkmate::assert_logical(auto_generated)
  mg_assert_timestamp_like(timestamp)

  # Unpack the db_table specification
  if (is.character(db_table)) {
    db_table_id <- mg_in_schema(db_table)
  } else {
    db_table_id <- dbplyr::remote_name(db_table)
    if (is.null(db_table_id)) stop('The remote_name of db_table could not be determined. Try giving as a character string ("schema.table") or direct query: tbl(conn, "schema.table") instead.')
  }
  db_schema     <- purrr::pluck(db_table_id, 1) %>% as.character()
  db_table_name <- purrr::pluck(db_table_id, 2) %>% as.character()

  # Pull current comments from the DB
  if (is.na(column)) {
    existing_comment <- dplyr::tbl(conn, mg_in_schema("information_schema", "tables")) %>%
      dplyr::filter(.data$table_schema == db_schema, .data$table_name == db_table_name) %>%
      tidyr::unite("db_table", "table_schema", "table_name", sep = ".", remove = FALSE) %>%
      dplyr::transmute(schema = .data$table_schema, table = .data$table_name, column_name = NA_character_,
                       comment = dbplyr::sql('obj_description(CAST("db_table" AS "regclass"))'))
  } else {
    existing_comment <- dplyr::tbl(conn, mg_in_schema("information_schema", "columns")) %>%
      dplyr::filter(.data$table_schema == db_schema, .data$table_name == db_table_name, .data$column_name == column) %>%
      tidyr::unite("db_table", "table_schema", "table_name", sep = ".", remove = FALSE) %>%
      dplyr::transmute(schema = .data$table_schema, table = .data$table_name, .data$column_name,
                       comment = dbplyr::sql('col_description(CAST("db_table" AS "regclass"), "ordinal_position")'))
  }

  # If comment is missing, pull comment from db
  if (is.null(comment)) {
    return(existing_comment %>% dplyr::pull("comment"))

  } else { # Update the comment in the DB

    # Look for un-escaped single quotes
    comment <- stringr::str_replace_all(comment, "(?<!\')\'(?!\')", "\'\'")

    # Check for reference to docs.template
    if (comment %like% "^@") {
      tmp <- stringr::str_split_fixed(comment, " ", n = 2)
      reference <- purrr::pluck(tmp, 1) %>% gsub("^@", "", .)
      append    <- purrr::pluck(tmp, 2)

      # Find the reference in docs.templates
      reference_comment <- mg_get_table(conn, "docs.templates") %>%
        dplyr::filter(.data$column_name == reference) %>%
        dplyr::pull(comment)

      # Check reference exists
      if (length(reference_comment) == 0) stop("Reference: '", reference, "' not found in docs.templates")

      # Update the comment
      comment <- paste(reference_comment, append) %>% trimws()
    }

    if (is.na(column)) {
      DBI::dbExecute(conn, glue::glue("COMMENT ON TABLE {db_schema}.{db_table_name} IS '{comment}'"))

    } else {
      DBI::dbExecute(conn, glue::glue("COMMENT ON COLUMN {db_schema}.{db_table_name}.{column} IS '{comment}'"))
    }

    # Push to documentation DB
    target_table <- "docs.documentation"

    # Create the documentation table if it does not exist
    if (!mg_table_exists(conn, target_table)) {
      invisible(log <- capture.output(mg_update_snapshot(existing_comment %>% dplyr::mutate(auto_generated = auto_generated) %>% head(0), conn, target_table, timestamp, log_file = FALSE)))
      failed <- log[!is.na(stringr::str_extract(log, "(WARNING|ERROR)"))]
      if (length(failed) > 0) print(failed)
    }

    # Update the record in the docs database
    invisible(log <- capture.output(
      mg_update_snapshot(
        existing_comment %>% dplyr::mutate(comment = !!comment, auto_generated = !!auto_generated),
        conn, target_table, timestamp,
        filters = existing_comment %>% dplyr::select(-"comment"),
        log_file = FALSE, enforce_chronological_order = FALSE)))
    failed <- log[!is.na(stringr::str_extract(log, "(WARNING|ERROR)"))]
    if (length(failed) > 0) print(failed)
  }
}

#' Add comments to database table from template
#'
#' @template conn
#' @template db_table
#' @param timestamp    Timestamp indicating when the comment is valid from
#' @return NULL
#' @importFrom rlang .data
#' @export
auto_comment <- function(conn, db_table, timestamp = glue::glue("{today()} 09:00:00")) {

  # Check arguments
  checkmate::assert_class(conn, "DBIConnection")
  mg_assert_dbtable_like(db_table)
  mg_assert_timestamp_like(timestamp)

  # Unpack the db_table specification
  if (is.character(db_table)) {
    db_table_id <- mg_in_schema(db_table)
  } else {
    db_table_id <- dbplyr::remote_name(db_table)
    if (is.null(db_table_id)) stop('The remote_name of db_table could not be determined. Try giving as a character string ("schema.table") or direct query: tbl(conn, "schema.table") instead.')
  }
  db_schema     <- purrr::pluck(db_table_id, 1) %>% as.character()
  db_table_name <- purrr::pluck(db_table_id, 2) %>% as.character()

  # Get existing auto-generated comments and missing comments
  elligble_comments <- dplyr::tbl(conn, mg_in_schema("information_schema", "columns")) %>%
      dplyr::filter(.data$table_schema == db_schema, .data$table_name == db_table_name) %>%
      tidyr::unite("db_table", "table_schema", "table_name", sep = ".", remove = FALSE) %>%
      dplyr::transmute(schema = .data$table_schema, table = .data$table_name, .data$column_name,
                       comment = dbplyr::sql('col_description(CAST("db_table" AS "regclass"), "ordinal_position")')) %>%
    mg_left_join(mg_get_table(conn, "docs.documentation"), by = c("schema", "table", "comment"), na_by = "column_name") %>%
    dplyr::filter(is.na(comment) | .data$auto_generated | comment == "NA")

  # Determine auto comments from templates
  auto_comments <- elligble_comments %>%
    dplyr::select(-"comment") %>%
    mg_left_join(mg_get_table(conn, "docs.templates"), by = "column_name") %>%
    dplyr::filter(!is.na(comment)) %>%
    tidyr::unite("db_table", "schema", "table", sep = ".", remove = TRUE)

  # Push comments to DB
  purrr::pwalk(dplyr::collect(auto_comments),
       ~ mg_db_comment(conn, ..4, column = ..1, comment = ..3, auto_generated = TRUE, timestamp = timestamp))
}
