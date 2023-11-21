test_that("SCDB gives too many messages", {
  conn <- DBI::dbConnect(RSQLite::SQLite())

  target_schema <- "test_ds"

  test_id <- SCDB::id(paste(target_schema, "mtcars", sep = "."), conn)
  DBI::dbWriteTable(conn, test_id, mtcars)
  expect_message(SCDB::get_table(conn, test_id), "If you want to specify a schema use")
  # If this message disappears, we need to remove some "suppressMessages" in the code
  SCDB::get_tables(conn)

  DBI::dbDisconnect(conn)
})
