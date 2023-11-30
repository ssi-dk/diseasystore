test_that("DiseasystoreBase works", {

  # Test different initialization of the base module

  # 1)
  expect_error(DiseasystoreBase$new(), regexp = "source_conn option not defined")

  # 2)
  expect_error(DiseasystoreBase$new(source_conn = "/some/path"), regexp = "target_conn option not defined")

  # 3)
  withr::local_options("diseasystore.source_conn" = "/some/path")
  expect_error(DiseasystoreBase$new(), regexp = "target_conn option not defined")
  withr::local_options("diseasystore.source_conn" = NULL)

  # 4)
  ds <- DiseasystoreBase$new(
    source_conn = "/some/path",
    target_conn = dbplyr::simulate_dbi()
  )
  expect_null(ds %.% start_date)
  expect_null(ds %.% end_date)
  rm(ds)

  # 5)
  ds <- DiseasystoreBase$new(
    source_conn = "/some/path",
    target_conn = dbplyr::simulate_dbi(),
    start_date = as.Date("2020-03-01")
  )
  expect_identical(ds %.% start_date, as.Date("2020-03-01"))
  expect_null(ds %.% end_date)
  rm(ds)

  # 6)
  ds <- DiseasystoreBase$new(
    source_conn = "/some/path",
    target_conn = dbplyr::simulate_dbi(),
    start_date = as.Date("2020-03-01"),
    end_date   = as.Date("2020-06-01")
  )
  expect_identical(ds %.% start_date, as.Date("2020-03-01"))
  expect_identical(ds %.% end_date,   as.Date("2020-06-01"))
  rm(ds)

  # 7)
  ds <- DiseasystoreBase$new(
    source_conn = "/some/path",
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
    source_conn = "/some/path",
    target_conn = dbplyr::simulate_dbi()
  )
  expect_identical(ds %.% target_schema, options() %.% "diseasystore.target_schema")
  rm(ds)

  # 9)
  ds <- DiseasystoreBase$new(
    source_conn = "/some/path",
    target_conn = dbplyr::simulate_dbi(),
    target_schema = "test_ds"
  )
  expect_identical(ds %.% target_schema, "test_ds")
  rm(ds)

  # 10)
  withr::local_options("diseasystore.target_schema" = "test_ds")
  ds <- DiseasystoreBase$new(
    source_conn = "/some/path",
    target_conn = dbplyr::simulate_dbi()
  )
  expect_identical(ds %.% target_schema, "test_ds")
  rm(ds)

  # 11)
  ds <- DiseasystoreBase$new(
    source_conn = "/some/path",
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


test_that("DiseasystoreBase$determine_new_ranges works", {

  start_date <- as.Date("2020-01-01")
  end_date   <- as.Date("2020-03-01")
  slice_ts <- glue::glue("{Sys.Date()} 09:00:00")

  conn <- DBI::dbConnect(RSQLite::SQLite())
  ds <- DiseasystoreBase$new(source_conn = "", target_conn = conn, target_schema = target_schema_1)
  logs <- suppressMessages(SCDB::create_logs_if_missing(paste(target_schema_1, "logs", sep = "."), conn))
  dplyr::rows_append(
    logs,
    data.frame(date = slice_ts,
               table = "table1",
               message = glue::glue("ds-range: {start_date} - {end_date}"),
               success = TRUE,
               log_file = "1"),
    copy = TRUE, in_place = TRUE)

  determine_new_ranges <- ds$.__enclos_env__$private$determine_new_ranges

  expect_equal(determine_new_ranges("table1", start_date, end_date, slice_ts),
               tibble::tibble(start_date = as.Date(character(0)),
                              end_date   = as.Date(character(0))))

  expect_equal(determine_new_ranges("table2", start_date, end_date, slice_ts),
               tibble::tibble(start_date = !!start_date,
                              end_date   = !!end_date))

  expect_equal(determine_new_ranges("table1", start_date, end_date + lubridate::days(5), slice_ts),
               tibble::tibble(start_date = !!end_date + lubridate::days(1),
                              end_date   = !!end_date + lubridate::days(5)))

  expect_equal(determine_new_ranges("table1", start_date - lubridate::days(5), end_date, slice_ts),
               tibble::tibble(start_date = !!start_date - lubridate::days(5),
                              end_date   = !!start_date - lubridate::days(1)))

  expect_equal(determine_new_ranges("table1", start_date - lubridate::days(5), end_date + lubridate::days(5), slice_ts),
               tibble::tibble(start_date = c(!!start_date - lubridate::days(5), !!end_date + lubridate::days(1)),
                              end_date   = c(!!start_date - lubridate::days(1), !!end_date + lubridate::days(5))))

  expect_equal(determine_new_ranges("table1", start_date - lubridate::days(5), end_date + lubridate::days(3), slice_ts),
               tibble::tibble(start_date = c(!!start_date - lubridate::days(5), !!end_date + lubridate::days(1)),
                              end_date   = c(!!start_date - lubridate::days(1), !!end_date + lubridate::days(3))))
})


test_that("active binding: ds_map works", {
  m <- DiseasystoreBase$new(source_conn = "", target_conn = dbplyr::simulate_dbi())

  # Retrieve the ds_map
  expect_equal(m$ds_map, NULL)

  # Try to set the ds_map
  expect_equal(tryCatch(m$ds_map <- list("testing" = "n_positive"), error = \(e) e),
               simpleError("`$ds_map` is read only"))
  expect_equal(m$ds_map, NULL)
  rm(m)
})


test_that("active binding: available_features works", {
  m <- DiseasystoreBase$new(source_conn = "", target_conn = dbplyr::simulate_dbi())

  # Retrieve the available_features
  expect_equal(m$available_features, NULL)

  # Try to set the available_features
  # test_that cannot capture this error, so we have to hack it
  expect_equal(tryCatch(m$available_features <- list("n_test", "n_positive"), error = \(e) e),
               simpleError("`$available_features` is read only"))
  expect_equal(m$available_features, NULL)
  rm(m)
})


test_that("active binding: label works", {
  m <- DiseasystoreBase$new(source_conn = "", target_conn = dbplyr::simulate_dbi())

  # Retrieve the label
  expect_equal(m$label, NULL)

  # Try to set the label
  expect_equal(tryCatch(m$label <- "test", error = \(e) e),
               simpleError("`$label` is read only"))
  expect_equal(m$label, NULL)
  rm(m)
})
