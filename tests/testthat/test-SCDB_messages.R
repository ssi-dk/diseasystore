withr::local_options("diseasystore.target_schema" = target_schema_1)

test_that("SCDB gives too many messages", {

  # The following SCDB functions have been giving unwanted messages
  # SCDB::get_tables
  # SCDB::is_historical

  # As well as calls to dplyr::tbl if a SCDB::id is supplied

  # Here we test whether these still give messages



  # SCDB::get_tables -- privileges warning (tempfile)
  conn <- DBI::dbConnect(RSQLite::SQLite(), dbname = tempfile())
  expect_warning(SCDB::get_tables(conn), "Check user privileges / database configuration")
  dplyr::copy_to(conn, iris, name = "iris")
  DBI::dbDisconnect(conn)


  conn <- DBI::dbConnect(RSQLite::SQLite())

  # SCDB::get_tables -- privileges warning (memory)
  expect_warning(SCDB::get_tables(conn), "Check user privileges / database configuration")
  dplyr::copy_to(conn, iris, name = "iris")

  target_schema <- diseasyoption("target_schema")
  test_id <- SCDB::id(paste(target_schema, "mtcars", sep = "."), conn)

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
