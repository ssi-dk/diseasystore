test_that("drop_diseasystore can delete entire default schema", {

  conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")

  # Create logs table in "ds" schema and add mtcars to the schema
  # to simulate a diseasystore on the connection
  SCDB::create_logs_if_missing("ds.logs", conn)
  SCDB::create_table(mtcars, conn, "ds.mtcars_1")
  SCDB::create_table(mtcars, conn, "ds.mtcars_2")

  # Add some other tables to simulate data that should not be touched
  SCDB::create_table(mtcars, conn, "mtcars_1")
  SCDB::create_table(mtcars, conn, "not_ds.mtcars_1")

  # Try to delete the entire ds store
  drop_diseasystore(conn = conn)

  expect_false(SCDB::table_exists(conn, "ds.logs"))
  expect_false(SCDB::table_exists(conn, "ds.mtcars_1"))
  expect_false(SCDB::table_exists(conn, "ds.mtcars_2"))

  expect_true(SCDB::table_exists(conn, "mtcars_1"))
  expect_true(SCDB::table_exists(conn, "not_ds.mtcars_1"))

  DBI::dbDisconnect(conn)

})


test_that("drop_diseasystore can delete single table in default schema", {

  conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")

  # Create logs table in "ds" schema and add mtcars to the schema
  # to simulate a diseasystore on the connection
  SCDB::create_logs_if_missing("ds.logs", conn)
  SCDB::create_table(mtcars, conn, "ds.mtcars_1")
  SCDB::create_table(mtcars, conn, "ds.mtcars_2")

  # Add some other tables to simulate data that should not be touched
  SCDB::create_table(mtcars, conn, "mtcars_1")
  SCDB::create_table(mtcars, conn, "not_ds.mtcars_1")

  # Try to delete only mtcars_1 within the diseasystore
  drop_diseasystore(pattern = "mtcars_1", conn = conn)

  expect_true(SCDB::table_exists(conn, "ds.logs"))
  expect_false(SCDB::table_exists(conn, "ds.mtcars_1"))
  expect_true(SCDB::table_exists(conn, "ds.mtcars_2"))

  expect_true(SCDB::table_exists(conn, "mtcars_1"))
  expect_true(SCDB::table_exists(conn, "not_ds.mtcars_1"))

  # Try to delete only mtcars_2 within the diseasystore
  drop_diseasystore(pattern = "mtcars_2", conn = conn)

  expect_true(SCDB::table_exists(conn, "ds.logs"))
  expect_false(SCDB::table_exists(conn, "ds.mtcars_1"))
  expect_false(SCDB::table_exists(conn, "ds.mtcars_2"))

  expect_true(SCDB::table_exists(conn, "mtcars_1"))
  expect_true(SCDB::table_exists(conn, "not_ds.mtcars_1"))

  DBI::dbDisconnect(conn)

})


test_that("drop_diseasystore can delete entire non-default schema", {

  conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")

  # Create logs table in "ds" schema and add mtcars to the schema
  # to simulate a diseasystore on the connection
  SCDB::create_logs_if_missing("fs.logs", conn)
  SCDB::create_table(mtcars, conn, "fs.mtcars_1")
  SCDB::create_table(mtcars, conn, "fs.mtcars_2")

  # Add some other tables to simulate data that should not be touched
  SCDB::create_table(mtcars, conn, "mtcars_1")
  SCDB::create_table(mtcars, conn, "not_ds.mtcars_1")

  # Try to delete the entire diseasystore
  drop_diseasystore(schema = "fs", conn = conn)

  expect_false(SCDB::table_exists(conn, "fs.logs"))
  expect_false(SCDB::table_exists(conn, "fs.mtcars_1"))
  expect_false(SCDB::table_exists(conn, "fs.mtcars_2"))

  expect_true(SCDB::table_exists(conn, "mtcars_1"))
  expect_true(SCDB::table_exists(conn, "not_ds.mtcars_1"))

  DBI::dbDisconnect(conn)

})


test_that("drop_diseasystore can delete single table in non-default schema", {

  conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")

  # Create logs table in "fs" schema and add mtcars to the schema
  # to simulate a diseasystore on the connection
  SCDB::create_logs_if_missing("fs.logs", conn)
  SCDB::create_table(mtcars, conn, "fs.mtcars_1")
  SCDB::create_table(mtcars, conn, "fs.mtcars_2")

  # Add some other tables to simulate data that should not be touched
  SCDB::create_table(mtcars, conn, "mtcars_1")
  SCDB::create_table(mtcars, conn, "not_ds.mtcars_1")

  # Try to delete only mtcars_1 within the diseasystore
  drop_diseasystore(pattern = "mtcars_1", schema = "fs", conn = conn)

  expect_true(SCDB::table_exists(conn, "fs.logs"))
  expect_false(SCDB::table_exists(conn, "fs.mtcars_1"))
  expect_true(SCDB::table_exists(conn, "fs.mtcars_2"))

  expect_true(SCDB::table_exists(conn, "mtcars_1"))
  expect_true(SCDB::table_exists(conn, "not_ds.mtcars_1"))

  # Try to delete only mtcars_2 within the diseasystore
  drop_diseasystore(pattern = "mtcars_2", schema = "fs", conn = conn)

  expect_true(SCDB::table_exists(conn, "fs.logs"))
  expect_false(SCDB::table_exists(conn, "fs.mtcars_1"))
  expect_false(SCDB::table_exists(conn, "fs.mtcars_2"))

  expect_true(SCDB::table_exists(conn, "mtcars_1"))
  expect_true(SCDB::table_exists(conn, "not_ds.mtcars_1"))

  DBI::dbDisconnect(conn)

})
