withr::local_options("diseasystore.target_schema" = target_schema_1)

# Create a dummy DiseasystoreBase with a mtcars FeatureHandler
DiseasystoreDummy <- R6::R6Class(                                                                                     # nolint: object_name_linter
  classname = "DiseasystoreBase",
  inherit = DiseasystoreBase,
  private = list(
    .ds_map = list(
      "cyl" = "dummy_cyl",
      "vs" = "dummy_vs",
      "am" = "dummy_am"
    ),
    dummy_cyl = FeatureHandler$new(
      compute = function(start_date, end_date, slice_ts, source_conn) {
        return(
          dplyr::select(mtcars, "cyl") |>
            dplyr::transmute(
            "key_car" = rownames(mtcars),
            .data$cyl,
            valid_from = Sys.Date(),
            valid_until = as.Date(NA)
          )
        )
      }
    ),
    dummy_vs = FeatureHandler$new(
      compute = function(start_date, end_date, slice_ts, source_conn) {
        return(
          dplyr::select(mtcars, "vs") |>
            dplyr::transmute(
            "key_car" = rownames(mtcars),
            .data$vs,
            valid_from = Sys.Date(),
            valid_until = as.Date(NA)
          )
        )
      }
    ),
    dummy_am = FeatureHandler$new(
      compute = function(start_date, end_date, slice_ts, source_conn) {
        return(
          dplyr::select(mtcars, "am") |>
            dplyr::transmute(
            "key_car" = rownames(mtcars),
            .data$am,
            valid_from = Sys.Date(),
            valid_until = as.Date(NA)
          )
        )
      }
    )
  )
)


test_that("drop_diseasystore can delete entire default schema", {
  for (conn in get_test_conns()) {

    # Create a diseasystore to generate some data
    ds <- DiseasystoreDummy$new(
      target_conn = conn,
      start_date = as.Date("2020-01-01"),
      end_date = as.Date("2020-06-01"),
      verbose = FALSE
    )

    # Compute each feature
    ds$available_features |>
      utils::head(3) |>
      purrr::walk(~ ds$get_feature(.x))

    # And store the ids
    ds_ids <- ds$ds_map |>
      purrr::map(~ SCDB::id(paste(target_schema_1, ., sep = "."), conn)) |>
      purrr::keep(~ SCDB::table_exists(conn, .))
    ds_ids <- ds_ids[!duplicated(ds_ids)]

    # Get the id of the logs table
    logs_id <- SCDB::id(paste(target_schema_1, "logs", sep = "."), conn)

    # Then we create tables containing mtcars in other schemas which should be untouched by our tests
    non_ds_ids <- c(
      "mtcars_1",
      paste(target_schema_2, "mtcars_1", sep = ".")
    ) |>
      purrr::map(~ SCDB::id(., conn))

    purrr::walk(non_ds_ids, ~ SCDB::create_table(mtcars, conn, ., temporary = FALSE))


    # Try to delete the entire `target_schema_1` store
    # But first, verify that the testing target_schema has been set
    expect_identical(diseasyoption("target_schema"), target_schema_1)
    drop_diseasystore(conn = conn)

    expect_false(SCDB::table_exists(conn, logs_id))
    purrr::walk(ds_ids,     ~ expect_false(SCDB::table_exists(conn, .)))
    purrr::walk(non_ds_ids, ~ expect_true(SCDB::table_exists(conn, .)))


    # Make sure all tables have been removed to not interfere with other tests
    c(logs_id, ds_ids, non_ds_ids) |>
      purrr::walk(~ {
        if (SCDB::table_exists(conn, .)) {
          DBI::dbRemoveTable(conn, .)
        }
        expect_false(SCDB::table_exists(conn, .))
      })

    DBI::dbDisconnect(conn, shutdown = TRUE)
  }
  invisible(gc())
})


test_that("drop_diseasystore can delete single table in default schema", {
  for (conn in get_test_conns()) {

    # Create a diseasystore to generate some data
    ds <- DiseasystoreDummy$new(
      target_conn = conn,
      start_date = as.Date("2020-01-01"),
      end_date = as.Date("2020-06-01"),
      verbose = FALSE
    )

    # Compute each feature
    ds$available_features |>
      utils::head(3) |>
      purrr::walk(~ ds$get_feature(.x))

    # And store the ids
    ds_ids <- ds$ds_map |>
      purrr::map(~ SCDB::id(paste(target_schema_1, ., sep = "."), conn)) |>
      purrr::keep(~ SCDB::table_exists(conn, .))
    ds_ids <- ds_ids[!duplicated(ds_ids)]

    # Get the id of the logs table
    logs_id <- SCDB::id(paste(target_schema_1, "logs", sep = "."), conn)

    # Then we create tables containing mtcars in other schemas which should be untouched by our tests
    non_ds_ids <- c(
      "mtcars_1",
      paste(target_schema_2, "mtcars_1", sep = ".")
    ) |>
      purrr::map(~ SCDB::id(., conn))

    purrr::walk(non_ds_ids, ~ SCDB::create_table(mtcars, conn, ., temporary = FALSE))


    # Try to delete only the first feature within the diseasystore
    # But first, verify that the testing target_schema has been set
    expect_identical(diseasyoption("target_schema"), target_schema_1)
    drop_diseasystore(pattern = ds$ds_map[[names(ds_ids)[[1]]]], conn = conn)

    expect_true(SCDB::table_exists(conn, logs_id))
    expect_false(SCDB::table_exists(conn, ds_ids[[1]]))
    purrr::walk(utils::tail(ds_ids, -1), ~ expect_true(SCDB::table_exists(conn, .)))
    purrr::walk(non_ds_ids,              ~ expect_true(SCDB::table_exists(conn, .)))

    # Try to delete also the second feature within the diseasystore
    # But first, verify that the testing target_schema has been set
    expect_identical(diseasyoption("target_schema"), target_schema_1)
    drop_diseasystore(pattern = ds$ds_map[[names(ds_ids)[[2]]]], conn = conn)

    expect_true(SCDB::table_exists(conn, logs_id))
    purrr::walk(utils::head(ds_ids, 2),  ~ expect_false(SCDB::table_exists(conn, .)))
    purrr::walk(utils::tail(ds_ids, -3), ~ expect_true(SCDB::table_exists(conn, .)))
    purrr::walk(non_ds_ids,       ~ expect_true(SCDB::table_exists(conn, .)))

    # Make sure all tables have been removed to not interfere with other tests
    c(logs_id, ds_ids, non_ds_ids) |>
      purrr::walk(~ {
        if (SCDB::table_exists(conn, .)) {
          DBI::dbRemoveTable(conn, .)
        }
        expect_false(SCDB::table_exists(conn, .))
      })

    DBI::dbDisconnect(conn, shutdown = TRUE)
  }
  invisible(gc())
})
