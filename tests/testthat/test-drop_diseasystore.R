test_that("drop_diseasystore can delete entire default schema", { for (conn in get_test_conns()) { # nolint: brace_linter

  # Create logs table in "test_ds" schema and add mtcars to the schema
  # to simulate a diseasystore on the connection
  suppressMessages(SCDB::create_logs_if_missing("test_ds.logs", conn))
  suppressMessages(SCDB::create_table(mtcars, conn, "test_ds.mtcars_1", temporary = FALSE))
  suppressMessages(SCDB::create_table(mtcars, conn, "test_ds.mtcars_2", temporary = FALSE))

  # Add some other tables to simulate data that should not be touched
  suppressMessages(SCDB::create_table(mtcars, conn, "mtcars_1",             temporary = FALSE))
  suppressMessages(SCDB::create_table(mtcars, conn, "not_test_ds.mtcars_1", temporary = FALSE))

  # Try to delete the entire test_ds store
  expect_equal(diseasyoption("target_schema"), "test_ds")      # Verify, that the testing target_schema has been set
  drop_diseasystore(conn = conn)

  expect_false(SCDB::table_exists(conn, "test_ds.logs"))
  expect_false(SCDB::table_exists(conn, "test_ds.mtcars_1"))
  expect_false(SCDB::table_exists(conn, "test_ds.mtcars_2"))

  expect_true(SCDB::table_exists(conn, "mtcars_1"))
  expect_true(SCDB::table_exists(conn, "not_test_ds.mtcars_1"))


  # Make sure all tables have been removed
  c("test_ds.logs", "test_ds.mtcars_1", "test_ds.mtcars_2", "mtcars_1", "not_test_ds.mtcars_1") |>
    purrr::walk(~ {
      if (SCDB::table_exists(conn, .)) {
        DBI::dbRemoveTable(conn, SCDB::id(., conn))
      }
      expect_false(SCDB::table_exists(conn, .))
    })
}})


test_that("drop_diseasystore can delete single table in default schema", { for (conn in get_test_conns()) { # nolint: brace_linter

  # Create logs table in "test_ds" schema and add mtcars to the schema
  # to simulate a diseasystore on the connection
  suppressMessages(SCDB::create_logs_if_missing("test_ds.logs", conn))
  suppressMessages(SCDB::create_table(mtcars, conn, "test_ds.mtcars_1", temporary = FALSE))
  suppressMessages(SCDB::create_table(mtcars, conn, "test_ds.mtcars_2", temporary = FALSE))

  # Add some other tables to simulate data that should not be touched
  suppressMessages(SCDB::create_table(mtcars, conn, "mtcars_1",             temporary = FALSE))
  suppressMessages(SCDB::create_table(mtcars, conn, "not_test_ds.mtcars_1", temporary = FALSE))

  # Try to delete only mtcars_1 within the diseasystore
  expect_equal(diseasyoption("target_schema"), "test_ds")      # Verify, that the testing target_schema has been set
  drop_diseasystore(pattern = "mtcars_1", conn = conn)

  expect_true(SCDB::table_exists(conn, "test_ds.logs"))
  expect_false(SCDB::table_exists(conn, "test_ds.mtcars_1"))
  expect_true(SCDB::table_exists(conn, "test_ds.mtcars_2"))

  expect_true(SCDB::table_exists(conn, "mtcars_1"))
  expect_true(SCDB::table_exists(conn, "not_test_ds.mtcars_1"))

  # Try to delete only mtcars_2 within the diseasystore
  expect_equal(diseasyoption("target_schema"), "test_ds")      # Verify, that the testing target_schema has been set
  drop_diseasystore(pattern = "mtcars_2", conn = conn)

  expect_true(SCDB::table_exists(conn, "test_ds.logs"))
  expect_false(SCDB::table_exists(conn, "test_ds.mtcars_1"))
  expect_false(SCDB::table_exists(conn, "test_ds.mtcars_2"))

  expect_true(SCDB::table_exists(conn, "mtcars_1"))
  expect_true(SCDB::table_exists(conn, "not_test_ds.mtcars_1"))

  # Make sure all tables have been removed
  c("test_ds.logs", "test_ds.mtcars_1", "test_ds.mtcars_2", "mtcars_1", "not_test_ds.mtcars_1") |>
    purrr::walk(~ {
      if (SCDB::table_exists(conn, .)) {
        DBI::dbRemoveTable(conn, SCDB::id(., conn))
      }
      expect_false(SCDB::table_exists(conn, .))
    })
}})
