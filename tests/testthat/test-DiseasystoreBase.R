# Set testing options
withr::local_options("diseasystore.target_schema" = target_schema_1)
withr::local_options("diseasystore.lock_wait_max" = 1 * 60) # 1 minute during tests

test_that("DiseasystoreBase works", {

  # Test different initialization of the base module

  # 1)
  expect_error(DiseasystoreBase$new(), regexp = "source_conn option not defined")

  # 2)
  expect_error(DiseasystoreBase$new(source_conn = file.path("some", "path")), regexp = "target_conn option not defined")

  # 3)
  withr::local_options("diseasystore.source_conn" = file.path("some", "path"))
  expect_error(DiseasystoreBase$new(), regexp = "target_conn option not defined")
  withr::local_options("diseasystore.source_conn" = NULL)

  # 4)
  ds <- DiseasystoreBase$new(
    source_conn = file.path("some", "path"),
    target_conn = dbplyr::simulate_dbi()
  )
  expect_null(ds %.% start_date)
  expect_null(ds %.% end_date)
  rm(ds)

  # 5)
  ds <- DiseasystoreBase$new(
    source_conn = file.path("some", "path"),
    target_conn = dbplyr::simulate_dbi(),
    start_date = as.Date("2020-03-01")
  )
  expect_identical(ds %.% start_date, as.Date("2020-03-01"))
  expect_null(ds %.% end_date)
  rm(ds)

  # 6)
  ds <- DiseasystoreBase$new(
    source_conn = file.path("some", "path"),
    target_conn = dbplyr::simulate_dbi(),
    start_date = as.Date("2020-03-01"),
    end_date   = as.Date("2020-06-01")
  )
  expect_identical(ds %.% start_date, as.Date("2020-03-01"))
  expect_identical(ds %.% end_date,   as.Date("2020-06-01"))
  rm(ds)

  # 7)
  ds <- DiseasystoreBase$new(
    source_conn = file.path("some", "path"),
    target_conn = dbplyr::simulate_dbi(),
    start_date = as.Date("2020-03-01"),
    end_date   = as.Date("2020-06-01"),
    slice_ts   = "2021-01-01 09:00:00"
  )
  expect_identical(ds %.% start_date, as.Date("2020-03-01"))
  expect_identical(ds %.% end_date,   as.Date("2020-06-01"))
  expect_identical(ds %.% slice_ts,   "2021-01-01 09:00:00")
  rm(ds)

  # 8)
  ds <- DiseasystoreBase$new(
    source_conn = file.path("some", "path"),
    target_conn = dbplyr::simulate_dbi()
  )
  expect_identical(ds %.% target_schema, getOption("diseasystore.target_schema"))
  rm(ds)

  # 9)
  ds <- DiseasystoreBase$new(
    source_conn = file.path("some", "path"),
    target_conn = dbplyr::simulate_dbi(),
    target_schema = "test_ds"
  )
  expect_identical(ds %.% target_schema, "test_ds")
  rm(ds)

  # 10)
  withr::local_options("diseasystore.target_schema" = "test_ds")
  ds <- DiseasystoreBase$new(
    source_conn = file.path("some", "path"),
    target_conn = dbplyr::simulate_dbi()
  )
  expect_identical(ds %.% target_schema, "test_ds")
  rm(ds)

  # 11)
  ds <- DiseasystoreBase$new(
    source_conn = file.path("some", "path"),
    target_conn = dbplyr::simulate_dbi(),
    target_schema = "test_ds"
  )
  expect_identical(ds %.% target_schema, "test_ds")
  rm(ds)

  # 12)
  ds <- DiseasystoreBase$new(target_conn = dbplyr::simulate_dbi())
  expect_identical(ds %.% target_conn, ds %.% source_conn)
  rm(ds)

})


test_that("$get_feature() verbosity works", {
  for (conn in get_test_conns()) {

    # Create a dummy DiseasystoreBase with a mtcars FeatureHandler
    DiseasystoreDummy <- R6::R6Class(                                                                                   # nolint: object_name_linter
      classname = "DiseasystoreBase",
      inherit = DiseasystoreBase,
      private = list(
        .ds_map = list("cyl" = "dummy_mtcars"),
        dummy_mtcars = FeatureHandler$new(
          compute = function(start_date, end_date, slice_ts, source_conn, ...) {
            return(dplyr::mutate(mtcars, valid_from = Sys.Date(), valid_until = as.Date(NA)))
          }
        )
      )
    )

    # Create an instance with verbose = TRUE
    ds <- DiseasystoreDummy$new(
      source_conn = file.path("some", "path"),
      target_conn = conn,
      verbose = TRUE
    )

    # Capture the messages from a get_feature call and compare with expectation
    messages <- capture.output(
      invisible(ds$get_feature("cyl", start_date = Sys.Date(), end_date = Sys.Date())),
      type = "message"
    )

    # We keep messages from `get_feature` which starts with "feature:".
    # Messages from other sources (duckdb or odbc) are not captured.
    messages <- purrr::discard(messages, ~ !startsWith(., "feature:"))

    checkmate::expect_character(messages[[1]], pattern = "feature: cyl needs to be computed on the specified date int.")
    checkmate::expect_character(messages[[2]], pattern = r"{feature: cyl updated \(elapsed time}")

    # Second identical call should give no messages
    messages <- capture.output(
      invisible(ds$get_feature("cyl", start_date = Sys.Date(), end_date = Sys.Date())),
      type = "message"
    )
    messages <- purrr::discard(messages, ~ !startsWith(., "feature:"))
    expect_identical(messages, character(0))

    rm(ds)
    DBI::dbDisconnect(conn)
  }

  invisible(gc())
})


test_that("$get_feature() works with different slice_ts data_types", {
  for (conn in get_test_conns()) {

    # Create a dummy DiseasystoreBase with a mtcars FeatureHandler
    DiseasystoreDummy <- R6::R6Class(                                                                                   # nolint: object_name_linter
      classname = "DiseasystoreBase",
      inherit = DiseasystoreBase,
      private = list(
        .ds_map = list("cyl" = "dummy_mtcars"),
        dummy_mtcars = FeatureHandler$new(
          compute = function(start_date, end_date, slice_ts, source_conn, ...) {
            return(dplyr::mutate(mtcars, valid_from = Sys.Date(), valid_until = as.Date(NA)))
          }
        )
      )
    )

    # Create an instance for tests
    ds <- DiseasystoreDummy$new(
      source_conn = file.path("some", "path"),
      target_conn = conn,
      verbose = FALSE
    )

    # Determine where these features are stored
    target_table <- SCDB::id(paste(ds %.% target_schema, "dummy_mtcars", sep = "."), ds %.% target_conn)

    # Get a reference to the logs table
    log_table_id <- SCDB::id(paste(ds %.% target_schema, "logs", sep = "."), ds %.% target_conn)

    # Create list of slice_ts to test
    slice_tss <- list(
      "Date" = Sys.Date(),
      "character Date" = as.character(Sys.Date()),
      "Integer Date" = as.Date(as.integer(Sys.Date())),
      "POSIXct" = Sys.time(),
      "character timestamp" = as.character(Sys.time())
    )

    for (slice_ts in slice_tss) {
      # Ensure we start from a clean state
      drop_diseasystore(conn = ds %.% target_conn, schema = ds %.% target_schema)

      log_table <- SCDB::create_logs_if_missing(log_table = log_table_id, conn = ds %.% target_conn)
      expect_identical(SCDB::nrow(log_table), 0L)

      # Check no data has been computed
      ds_missing_ranges <- ds$determine_missing_ranges(
        target_table = target_table,
        start_date   = Sys.Date(),
        end_date     = Sys.Date(),
        slice_ts     = slice_ts
      )
      expect_identical(SCDB::nrow(ds_missing_ranges), 1L)

      # Compute feature
      ds$get_feature("cyl", start_date = Sys.Date(), end_date = Sys.Date(), slice_ts = slice_ts)

      # Check data has been computed
      expect_identical(SCDB::nrow(log_table), 1L)

      # Check no missing ranges are returned after computation
      ds_missing_ranges <- ds$determine_missing_ranges(
        target_table = target_table,
        start_date   = Sys.Date(),
        end_date     = Sys.Date(),
        slice_ts     = slice_ts
      )
      testthat::expect_identical(SCDB::nrow(ds_missing_ranges), 0L)
    }

    rm(ds)
    DBI::dbDisconnect(conn)
  }

  invisible(gc())
})


test_that("DiseasystoreBase$determine_missing_ranges() works", {

  start_date <- as.Date("2020-01-01")
  end_date   <- as.Date("2020-03-01")

  for (conn in get_test_conns()) {
    ds <- DiseasystoreBase$new(source_conn = "", target_conn = conn)
    slice_ts <- ds %.% slice_ts

    logs <- SCDB::create_logs_if_missing(conn = conn, log_table = paste(target_schema_1, "logs", sep = "."))

    # Add entry for a fictional table1
    table1_entry <- data.frame(
      date = NA,
      table = "table1",
      message = glue::glue("ds-range: {start_date} - {end_date}"),
      success = TRUE,
      log_file = "1"
    ) |>
      dplyr::copy_to(conn, df = _, name = SCDB::unique_table_name("diseasystore"))
    SCDB::defer_db_cleanup(table1_entry)

    dplyr::rows_append(
      logs,
      dplyr::mutate(table1_entry, "date" = !!SCDB::db_timestamp(slice_ts, conn)),
      in_place = TRUE
    )


    # Start the tests
    expect_identical(
      ds$determine_missing_ranges("table1", start_date, end_date, ds %.% slice_ts),
      tibble::tibble(
        start_date = as.Date(character(0)),
        end_date   = as.Date(character(0))
      )
    )

    expect_identical(
      ds$determine_missing_ranges("table2", start_date, end_date, ds %.% slice_ts),
      tibble::tibble(
        start_date = !!start_date,
        end_date   = !!end_date
      )
    )

    expect_identical(
      ds$determine_missing_ranges("table1", start_date, end_date + lubridate::days(5), ds %.% slice_ts),
      tibble::tibble(
        start_date = !!end_date + lubridate::days(1),
        end_date   = !!end_date + lubridate::days(5)
      )
    )

    expect_identical(
      ds$determine_missing_ranges("table1", start_date - lubridate::days(5), end_date, ds %.% slice_ts),
      tibble::tibble(
        start_date = !!start_date - lubridate::days(5),
        end_date   = !!start_date - lubridate::days(1)
      )
    )

    expect_identical(
      ds$determine_missing_ranges(
        "table1",
        start_date - lubridate::days(5),
        end_date + lubridate::days(5),
        ds %.% slice_ts
      ),
      tibble::tibble(
        start_date = c(!!start_date - lubridate::days(5), !!end_date + lubridate::days(1)),
        end_date   = c(!!start_date - lubridate::days(1), !!end_date + lubridate::days(5))
      )
    )

    expect_identical(
      ds$determine_missing_ranges(
        "table1",
        start_date - lubridate::days(5),
        end_date + lubridate::days(3),
        ds %.% slice_ts
      ),
      tibble::tibble(
        start_date = c(!!start_date - lubridate::days(5), !!end_date + lubridate::days(1)),
        end_date   = c(!!start_date - lubridate::days(1), !!end_date + lubridate::days(3))
      )
    )

    rm(ds)
    DBI::dbDisconnect(conn)
  }

  invisible(gc())
})


test_that("$key_join_features works with non-computing stratifications", {
  skip_if_not_installed("RSQLite")

  # Create a dummy, mtcars diseasystore with two features
  DiseasystoreDummy <- R6::R6Class(                                                                                     # nolint: object_name_linter
    classname = "DiseasystoreBase",
    inherit = DiseasystoreBase,
    private = list(
      .ds_map = list("n_cyl" = "dummy_cyl", "vs" = "dummy_vs"),
      dummy_cyl = FeatureHandler$new(
        compute = function(start_date, end_date, slice_ts, source_conn, ...) {
          out <- mtcars |>
            dplyr::mutate(
              "row_id" = dplyr::row_number(),
              "car" = paste(rownames(mtcars), .data$row_id)
            ) |>
            dplyr::transmute(
              "key_car" = .data$car, "n_cyl" = .data$cyl,
              "valid_from" = Sys.Date() - lubridate::days(2 * .data$row_id - 1),
              "valid_until" = .data$valid_from + lubridate::days(2)
            )
          return(out)
        },
        key_join = key_join_sum
      ),
      dummy_vs = FeatureHandler$new(
        compute = function(start_date, end_date, slice_ts, source_conn, ...) {
          out <- mtcars |>
            dplyr::mutate(
              "row_id" = dplyr::row_number(),
              "car" = paste(rownames(mtcars), .data$row_id)
            ) |>
            dplyr::transmute(
              "key_car" = .data$car, .data$vs,
              "valid_from" = Sys.Date() - lubridate::days(2 * .data$row_id),
              "valid_until" = .data$valid_from + lubridate::days(2)
            )
          return(out)
        },
        key_join = key_join_sum
      )
    )
  )

  # Create new instance
  ds <- DiseasystoreDummy$new(target_conn = DBI::dbConnect(RSQLite::SQLite()), verbose = FALSE)

  # Check that we can join the features even if stratification does not compute on features
  expect_no_error(
    ds$key_join_features(
      observable = "n_cyl",
      stratification = rlang::quos(vs = "test"),
      start_date = Sys.Date() - lubridate::days(2 * nrow(mtcars) - 1),
      end_date = Sys.Date()
    )
  )

})


test_that("active binding: ds_map works", {
  ds <- DiseasystoreBase$new(source_conn = "", target_conn = dbplyr::simulate_dbi())

  # Retrieve the ds_map
  expect_null(ds$ds_map)

  # Try to set the ds_map
  expect_identical(tryCatch(ds$ds_map <- list("testing" = "n_positive"), error = \(e) e),                               # nolint: implicit_assignment_linter
                   simpleError("`$ds_map` is read only"))
  expect_null(ds$ds_map)
  rm(ds)
})


test_that("active binding: available_features works", {
  ds <- DiseasystoreBase$new(source_conn = "", target_conn = dbplyr::simulate_dbi())

  # Retrieve the available_features
  expect_null(ds$available_features)

  # Try to set the available_features
  # test_that cannot capture this error, so we have to hack it
  expect_identical(tryCatch(ds$available_features <- list("n_test", "n_positive"), error = \(e) e),                     # nolint: implicit_assignment_linter
                   simpleError("`$available_features` is read only"))
  expect_null(ds$available_features)
  rm(ds)
})


test_that("active binding: label works", {
  ds <- DiseasystoreBase$new(source_conn = "", target_conn = dbplyr::simulate_dbi())

  # Retrieve the label
  expect_null(ds$label)

  # Try to set the label
  expect_identical(tryCatch(ds$label <- "test", error = \(e) e),                                                        # nolint: implicit_assignment_linter
                   simpleError("`$label` is read only"))
  expect_null(ds$label)
  rm(ds)
})
