test_that("SCDB gives too many messages", {

  # The following SCDB functions have been giving unwanted messages
  # SCDB::create_table
  # SCDB::create_logs_if_missing
  # SCDB::get_table
  # SCDB::update_snapshot
  # SCDB::is_historical

  # As well as calls to dplyr::tbl if a SCDB::id is supplied

  # Here we test whether these still give messages

  conn <- DBI::dbConnect(RSQLite::SQLite())

  target_schema <- diseasyoption("target_schema", "diseasystore")
  test_id <- SCDB::id(paste(target_schema, "mtcars", sep = "."), conn)

  # SCDB::create_table
  expect_message(SCDB::create_table(mtcars, conn, test_id), "If you want to specify a schema use")

  # SCDB::create_logs_if_missing
  log_id <- SCDB::id(paste(target_schema, "logs", sep = "."), conn)
  expect_message(SCDB::create_logs_if_missing(log_id, conn), "If you want to specify a schema use")

  # SCDB::get_table
  expect_message(SCDB::get_table(conn, test_id), "If you want to specify a schema use")

  # SCDB::update_snapshot
  mtcars_remote <- dplyr::copy_to(conn, mtcars)
  expect_message(SCDB::update_snapshot(mtcars_remote, conn, paste(target_schema, "mtcars", sep = "."),
                                       timestamp = Sys.time(),
                                       logger = SCDB::Logger$new(output_to_console = FALSE, warn = FALSE)),
                 "If you want to specify a schema use")

  # SCDB::is_historical
  expect_message(SCDB::is.historical(dplyr::tbl(conn, test_id)), "If you want to specify a schema use")

  # If any of these message disappears, we need to remove some "suppressMessages" in the code

  DBI::dbDisconnect(conn)
})
