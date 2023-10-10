test_that("DiseasystoreBase works", {

  # Test different initialization of the base module

  # 1)
  expect_error(DiseasystoreBase$new(), regexp = "source_conn option not defined")

  # 2)
  expect_error(DiseasystoreBase$new(source_conn = "/some/path"), regexp = "target_conn option not defined")

  # 3)
  options("diseasystore.source_conn" = "/some/path")
  expect_error(DiseasystoreBase$new(), regexp = "target_conn option not defined")
  options("diseasystore.source_conn" = NULL)

  # 4)
  fs <- DiseasystoreBase$new(source_conn = "/some/path", target_conn = dbplyr::simulate_dbi())
  expect_null(fs %.% start_date)
  expect_null(fs %.% end_date)
  rm(fs)

  # 5)
  fs <- DiseasystoreBase$new(source_conn = "/some/path", target_conn = dbplyr::simulate_dbi(),
                             start_date = as.Date("2020-03-01"))
  expect_identical(fs %.% start_date, as.Date("2020-03-01"))
  expect_null(fs %.% end_date)
  rm(fs)

  # 6)
  fs <- DiseasystoreBase$new(source_conn = "/some/path", target_conn = dbplyr::simulate_dbi(),
                             start_date = as.Date("2020-03-01"),
                             end_date   = as.Date("2020-06-01"))
  expect_identical(fs %.% start_date, as.Date("2020-03-01"))
  expect_identical(fs %.% end_date,   as.Date("2020-06-01"))
  rm(fs)

  # 7)
  fs <- DiseasystoreBase$new(source_conn = "/some/path", target_conn = dbplyr::simulate_dbi(),
                             start_date = as.Date("2020-03-01"),
                             end_date   = as.Date("2020-06-01"),
                             slice_ts   = "2021-01-01 09:00:00")
  expect_identical(fs %.% start_date, as.Date("2020-03-01"))
  expect_identical(fs %.% end_date,   as.Date("2020-06-01"))
  expect_identical(fs %.% slice_ts,   "2021-01-01 09:00:00")
  rm(fs)

  # 8)
  fs <- DiseasystoreBase$new(source_conn = "/some/path", target_conn = dbplyr::simulate_dbi())
  expect_identical(fs %.% target_schema, "ds")
  rm(fs)

  # 9)
  fs <- DiseasystoreBase$new(source_conn = "/some/path", target_conn = dbplyr::simulate_dbi(), target_schema = "test")
  expect_identical(fs %.% target_schema, "test")
  rm(fs)

  # 10)
  options(diseasystore.target_schema = "test")
  fs <- DiseasystoreBase$new(source_conn = "/some/path", target_conn = dbplyr::simulate_dbi())
  expect_identical(fs %.% target_schema, "test")
  rm(fs)

  # 11)
  fs <- DiseasystoreBase$new(source_conn = "/some/path", target_conn = dbplyr::simulate_dbi(), target_schema = "test")
  expect_identical(fs %.% target_schema, "test")
  rm(fs)

  # 12)
  fs <- DiseasystoreBase$new(target_conn = dbplyr::simulate_dbi())
  expect_identical(fs %.% target_conn, fs %.% source_conn)
  rm(fs)

})


test_that("DiseasystoreBase$determine_new_ranges works", {

  start_date <- as.Date("2020-01-01")
  end_date   <- as.Date("2020-03-01")
  slice_ts <- glue::glue("{Sys.Date()} 09:00:00")

  conn <- DBI::dbConnect(RSQLite::SQLite())
  fs <- DiseasystoreBase$new(source_conn = "", target_conn = conn, target_schema = "test_ds")
  logs <- SCDB::create_logs_if_missing("test_ds.logs", conn)
  rows_append(logs, data.frame(date = slice_ts,
                               table = "test",
                               message = glue::glue("fs-range: {start_date} - {end_date}"),
                               log_file = "1"),
              copy = TRUE, in_place = TRUE)

  determine_new_ranges <- fs$.__enclos_env__$private$determine_new_ranges

  expect_equal(determine_new_ranges("test", start_date, end_date, slice_ts),
               tibble::tibble(start_date = as.Date(character(0)), end_date = as.Date(character(0))))

  expect_equal(determine_new_ranges("testing", start_date, end_date, slice_ts),
               tibble::tibble(start_date = !!start_date,
                              end_date   = !!end_date))

  expect_equal(determine_new_ranges("test", start_date, end_date + lubridate::days(1), slice_ts),
               tibble::tibble(start_date = !!end_date + lubridate::days(1),
                              end_date   = !!end_date + lubridate::days(1)))

  expect_equal(determine_new_ranges("test", start_date - lubridate::days(1), end_date, slice_ts),
               tibble::tibble(start_date = !!start_date - lubridate::days(1),
                              end_date   = !!start_date - lubridate::days(1)))

  expect_equal(determine_new_ranges("test", start_date - lubridate::days(1), end_date + lubridate::days(1), slice_ts),
               tibble::tibble(start_date = c(!!start_date - lubridate::days(1), !!end_date + lubridate::days(1)),
                              end_date   = c(!!start_date - lubridate::days(1), !!end_date + lubridate::days(1))))

  expect_equal(determine_new_ranges("test", start_date - lubridate::days(2), end_date + lubridate::days(2), slice_ts),
               tibble::tibble(start_date = c(!!start_date - lubridate::days(2), !!end_date + lubridate::days(1)),
                              end_date   = c(!!start_date - lubridate::days(1), !!end_date + lubridate::days(2))))
})


test_that("active binding: fs_map works", {
  m <- DiseasystoreBase$new(source_conn = "", target_conn = dbplyr::simulate_dbi())

  # Retrieve the fs_map
  expect_equal(m$fs_map, NULL)

  # Try to set the fs_map
  expect_equal(tryCatch(m$fs_map <- list("testing" = "n_positive"), error = \(e) e),
               simpleError("`$fs_map` is read only"))
  expect_equal(m$fs_map, NULL)
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


test_that("active binding: case_definition works", {
  m <- DiseasystoreBase$new(source_conn = "", target_conn = dbplyr::simulate_dbi())

  # Retrieve the case_definition
  expect_equal(m$case_definition, NULL)

  # Try to set the case_definition
  expect_equal(tryCatch(m$case_definition <- "test", error = \(e) e),
               simpleError("`$case_definition` is read only"))
  expect_equal(m$case_definition, NULL)
  rm(m)
})
