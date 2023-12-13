withr::local_options("diseasystore.target_schema" = target_schema_1)

test_that("SCDB gives too many messages", {

  # The following SCDB functions have been giving unwanted messages
  # SCDB::create_table
  # SCDB::create_logs_if_missing
  # SCDB::get_table
  # SCDB::update_snapshot
  # SCDB::is_historical

  # As well as calls to dplyr::tbl if a SCDB::id is supplied

  # Here we test whether these still give messages



  # SCDB::get_table -- privileges warning (tempfile)
  conn <- DBI::dbConnect(RSQLite::SQLite(), dbname = tempfile())
  expect_warning(SCDB::get_tables(conn), "Check user privileges / database configuration")
  dplyr::copy_to(conn, iris, name = "iris")



  conn <- DBI::dbConnect(RSQLite::SQLite())

  # SCDB::get_table -- privileges warning (memory)
  expect_warning(SCDB::get_tables(conn), "Check user privileges / database configuration")
  dplyr::copy_to(conn, iris, name = "iris")

  target_schema <- diseasyoption("target_schema")
  test_id <- SCDB::id(paste(target_schema, "mtcars", sep = "."), conn)

  # SCDB::create_table
  expect_message(SCDB::create_table(mtcars, conn, test_id), "If you want to specify a schema use")

  # SCDB::create_logs_if_missing
  log_id <- SCDB::id(paste(target_schema, "logs", sep = "."), conn)
  expect_message(SCDB::create_logs_if_missing(log_id, conn), "If you want to specify a schema use")

  # SCDB::update_snapshot
  mtcars_remote <- dplyr::copy_to(conn, mtcars)
  message <- capture.output(
    invisible(
      SCDB::update_snapshot(mtcars_remote, conn, paste(target_schema, "mtcars", sep = "."),
                            timestamp = Sys.time(),
                            logger = SCDB::Logger$new(output_to_console = FALSE, warn = FALSE))
    ),
    type = "message"
  ) |>
    paste(collapse = " ")
  checkmate::expect_character(message, pattern = "It looks like you tried to incorrectly use")

  # SCDB::is_historical
  message <- capture.output(
    invisible(SCDB::is.historical(dplyr::tbl(conn, test_id))),
    type = "message"
  ) |>
    paste(collapse = " ")
  checkmate::expect_character(message, pattern = "It looks like you tried to incorrectly use")


  # As well as calls to dplyr::tbl if a SCDB::id is supplied
  message <- capture.output(
    invisible(dplyr::tbl(conn, test_id)),
    type = "message"
  ) |>
    paste(collapse = " ")
  checkmate::expect_character(message, pattern = "It looks like you tried to incorrectly use")


  # If any of these message disappears, we need to remove some "suppressMessages" in the code

  DBI::dbDisconnect(conn)
})
