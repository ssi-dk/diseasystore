withr::local_options("diseasystore.target_schema" = target_schema_1)

test_that("drop_diseasystore can delete entire default schema", {
  for (conn in get_test_conns()) {

    # Create logs table in `target_schema_1` schema to simulate a diseasystore in the schema
    logs_id <- SCDB::id(paste(target_schema_1, "logs", sep = "."), conn)
    SCDB::create_logs_if_missing(logs_id, conn)

    # Then we create tables containing mtcars in both the schema we will drop (target_schema_1)
    # and in other places which should be untouched by our tests
    ids <- c(
      paste(target_schema_1, "mtcars_1", sep = "."),
      paste(target_schema_1, "mtcars_2", sep = "."),
      "mtcars_1",
      paste(target_schema_2, "mtcars_1", sep = ".")
    ) |>
      purrr::map(~ SCDB::id(., conn))


    for (id in ids) {
      if (packageVersion("SCDB") <= "0.3") {
        DBI::dbWriteTable(conn, id, mtcars, temporary = FALSE, overwrite = TRUE)
      } else {
        SCDB::create_table(mtcars, conn, id, temporary = FALSE)
      }
    }

    # Try to delete the entire `target_schema_1` store
    # But first, verify that the testing target_schema has been set
    expect_identical(diseasyoption("target_schema"), target_schema_1)
    drop_diseasystore(conn = conn)

    expect_false(SCDB::table_exists(conn, logs_id))
    expect_false(SCDB::table_exists(conn, ids[[1]]))
    expect_false(SCDB::table_exists(conn, ids[[2]]))

    expect_true(SCDB::table_exists(conn, ids[[3]]))
    expect_true(SCDB::table_exists(conn, ids[[4]]))


    # Make sure all tables have been removed
    c(SCDB::id(paste(target_schema_1, "logs", sep = "."), conn), ids) |>
      purrr::walk(~ {
        if (SCDB::table_exists(conn, .)) {
          DBI::dbRemoveTable(conn, .)
        }
        expect_false(SCDB::table_exists(conn, .))
      })

    DBI::dbDisconnect(conn)
  }
  invisible(gc())
})


test_that("drop_diseasystore can delete single table in default schema", {
  for (conn in get_test_conns()) {

    # Create logs table in `target_schema_1` schema to simulate a diseasystore in the schema
    logs_id <- SCDB::id(paste(target_schema_1, "logs", sep = "."), conn)
    SCDB::create_logs_if_missing(logs_id, conn)

    # Then we create tables containing mtcars in both the schema we will drop (target_schema_1)
    # and in other places which should be untouched by our tests
    ids <- c(
      paste(target_schema_1, "mtcars_1", sep = "."),
      paste(target_schema_1, "mtcars_2", sep = "."),
      "mtcars_1",
      paste(target_schema_2, "mtcars_1", sep = ".")
    ) |>
      purrr::map(~ SCDB::id(., conn))

    for (id in ids) {
      if (packageVersion("SCDB") <= "0.3") {
        DBI::dbWriteTable(conn, id, mtcars, temporary = FALSE, overwrite = TRUE)
      } else {
        SCDB::create_table(mtcars, conn, id, temporary = FALSE)
      }
    }

    # Try to delete only mtcars_1 within the diseasystore
    # But first, verify that the testing target_schema has been set
    expect_identical(diseasyoption("target_schema"), target_schema_1)
    drop_diseasystore(pattern = "mtcars_1", conn = conn)

    expect_true(SCDB::table_exists(conn, logs_id))
    expect_false(SCDB::table_exists(conn, ids[[1]]))
    expect_true(SCDB::table_exists(conn, ids[[2]]))

    expect_true(SCDB::table_exists(conn, ids[[3]]))
    expect_true(SCDB::table_exists(conn, ids[[4]]))

    # Try to delete only mtcars_2 within the diseasystore
    # But first, verify that the testing target_schema has been set
    expect_identical(diseasyoption("target_schema"), target_schema_1)
    drop_diseasystore(pattern = "mtcars_2", conn = conn)

    expect_true(SCDB::table_exists(conn, logs_id))
    expect_false(SCDB::table_exists(conn, ids[[1]]))
    expect_false(SCDB::table_exists(conn, ids[[2]]))

    expect_true(SCDB::table_exists(conn, ids[[3]]))
    expect_true(SCDB::table_exists(conn, ids[[4]]))

    # Make sure all tables have been removed
    c(SCDB::id(paste(target_schema_1, "logs", sep = "."), conn), ids) |>
      purrr::walk(~ {
        if (SCDB::table_exists(conn, .)) {
          DBI::dbRemoveTable(conn, .)
        }
        expect_false(SCDB::table_exists(conn, .))
      })

    DBI::dbDisconnect(conn)
  }
  invisible(gc())
})
