withr::local_options("diseasystore.target_schema" = target_schema_1)

test_that("drop_diseasystore can delete entire default schema", {
  for (conn in get_test_conns()) {

    # Create logs table in `target_schema_1` schema and add mtcars to the schema
    # to simulate a diseasystore on the connection
    suppressMessages(SCDB::create_logs_if_missing(paste(target_schema_1, "logs", sep = "."), conn))
    suppressMessages(SCDB::create_table(mtcars, conn, paste(target_schema_1, "mtcars_1", sep = "."), temporary = FALSE))
    suppressMessages(SCDB::create_table(mtcars, conn, paste(target_schema_1, "mtcars_2", sep = "."), temporary = FALSE))

    # Add some other tables to simulate data that should not be touched
    suppressMessages(SCDB::create_table(mtcars, conn, "mtcars_1",                                    temporary = FALSE))
    suppressMessages(SCDB::create_table(mtcars, conn, paste(target_schema_2, "mtcars_1", sep = "."), temporary = FALSE))

    # Try to delete the entire `target_schema_1` store
    # But first, verify that the testing target_schema has been set
    expect_identical(diseasyoption("target_schema"), target_schema_1)
    drop_diseasystore(conn = conn)

    expect_false(SCDB::table_exists(conn, paste(target_schema_1, "logs",     sep = ".")))
    expect_false(SCDB::table_exists(conn, paste(target_schema_1, "mtcars_1", sep = ".")))
    expect_false(SCDB::table_exists(conn, paste(target_schema_1, "mtcars_2", sep = ".")))

    expect_true(SCDB::table_exists(conn, "mtcars_1"))
    expect_true(SCDB::table_exists(conn, paste(target_schema_2, "mtcars_1", sep = ".")))


    # Make sure all tables have been removed
    c(paste(target_schema_1, "logs",     sep = "."),
      paste(target_schema_1, "mtcars_1", sep = "."),
      paste(target_schema_1, "mtcars_2", sep = "."),
      "mtcars_1",
      paste(target_schema_2, "mtcars_1", sep = ".")
    ) |>
      purrr::walk(~ {
        if (SCDB::table_exists(conn, .)) {
          DBI::dbRemoveTable(conn, SCDB::id(., conn))
        }
        expect_false(SCDB::table_exists(conn, .))
      })

    DBI::dbDisconnect(conn)
  }
})


test_that("drop_diseasystore can delete single table in default schema", {
  for (conn in get_test_conns()) {

    # Create logs table in `target_schema_1` schema and add mtcars to the schema
    # to simulate a diseasystore on the connection
    suppressMessages(SCDB::create_logs_if_missing(paste(target_schema_1, "logs", sep = "."), conn))
    suppressMessages(SCDB::create_table(mtcars, conn, paste(target_schema_1, "mtcars_1", sep = "."), temporary = FALSE))
    suppressMessages(SCDB::create_table(mtcars, conn, paste(target_schema_1, "mtcars_2", sep = "."), temporary = FALSE))

    # Add some other tables to simulate data that should not be touched
    suppressMessages(SCDB::create_table(mtcars, conn, "mtcars_1",                                    temporary = FALSE))
    suppressMessages(SCDB::create_table(mtcars, conn, paste(target_schema_2, "mtcars_1", sep = "."), temporary = FALSE))

    # Try to delete only mtcars_1 within the diseasystore
    # But first, verify that the testing target_schema has been set
    expect_identical(diseasyoption("target_schema"), target_schema_1)
    drop_diseasystore(pattern = "mtcars_1", conn = conn)

    expect_true(SCDB::table_exists(conn, paste(target_schema_1, "logs", sep = ".")))
    expect_false(SCDB::table_exists(conn, paste(target_schema_1, "mtcars_1", sep = ".")))
    expect_true(SCDB::table_exists(conn, paste(target_schema_1, "mtcars_2", sep = ".")))

    expect_true(SCDB::table_exists(conn, "mtcars_1"))
    expect_true(SCDB::table_exists(conn, paste(target_schema_2, "mtcars_1", sep = ".")))

    # Try to delete only mtcars_2 within the diseasystore
    # But first, verify that the testing target_schema has been set
    expect_identical(diseasyoption("target_schema"), target_schema_1)
    drop_diseasystore(pattern = "mtcars_2", conn = conn)

    expect_true(SCDB::table_exists(conn, paste(target_schema_1, "logs", sep = ".")))
    expect_false(SCDB::table_exists(conn, paste(target_schema_1, "mtcars_1", sep = ".")))
    expect_false(SCDB::table_exists(conn, paste(target_schema_1, "mtcars_2", sep = ".")))

    expect_true(SCDB::table_exists(conn, "mtcars_1"))
    expect_true(SCDB::table_exists(conn, paste(target_schema_2, "mtcars_1", sep = ".")))

    # Make sure all tables have been removed
    c(paste(target_schema_1, "logs",     sep = "."),
      paste(target_schema_1, "mtcars_1", sep = "."),
      paste(target_schema_1, "mtcars_2", sep = "."),
      "mtcars_1",
      paste(target_schema_2, "mtcars_1", sep = ".")
    ) |>
      purrr::walk(~ {
        if (SCDB::table_exists(conn, .)) {
          DBI::dbRemoveTable(conn, SCDB::id(., conn))
        }
        expect_false(SCDB::table_exists(conn, .))
      })

    DBI::dbDisconnect(conn)
  }
})
