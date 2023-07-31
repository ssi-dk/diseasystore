test_that("DiseasystoreBase works", {

  # Test different initialization of the base module
  # 1)
  expect_error(DiseasystoreBase$new(), regexp = "source_conn option not defined")

  # 2)
  expect_error(DiseasystoreBase$new(source_conn = ""), regexp = "target_conn option not defined")

  # 3)
  options(diseasystore.source_conn = "")
  expect_error(DiseasystoreBase$new(), regexp = "target_conn option not defined")

  # 4)
  fs <- DiseasystoreBase$new(source_conn = "", target_conn = "")
  expect_null(fs$.__enclos_env__$private$start_date)
  expect_null(fs$.__enclos_env__$private$end_date)
  rm(fs)

  # 5)
  fs <- DiseasystoreBase$new(source_conn = "", target_conn = "",
                             start_date = as.Date("2020-03-01"))
  expect_identical(fs$.__enclos_env__$private$start_date, as.Date("2020-03-01"))
  expect_null(fs$.__enclos_env__$private$end_date)
  rm(fs)

  # 6)
  fs <- DiseasystoreBase$new(source_conn = "", target_conn = "",
                             start_date = as.Date("2020-03-01"),
                             end_date   = as.Date("2020-06-01"))
  expect_identical(fs$.__enclos_env__$private$start_date, as.Date("2020-03-01"))
  expect_identical(fs$.__enclos_env__$private$end_date,   as.Date("2020-06-01"))
  rm(fs)

  # 7)
  fs <- DiseasystoreBase$new(source_conn = "", target_conn = dbplyr::simulate_postgres())
  expect_identical(fs$.__enclos_env__$private$target_schema, "ds")
  rm(fs)

  # 8)
  fs <- DiseasystoreBase$new(source_conn = "", target_conn = dbplyr::simulate_postgres(), target_schema = "test")
  expect_identical(fs$.__enclos_env__$private$target_schema, "test")
  rm(fs)

  # 9)
  options(diseasystore.target_schema = "test")
  fs <- DiseasystoreBase$new(source_conn = "", target_conn = dbplyr::simulate_postgres())
  expect_identical(fs$.__enclos_env__$private$target_schema, "test")
  rm(fs)

  # 10)
  fs <- DiseasystoreBase$new(source_conn = "", target_conn = "", target_schema = "test")
  expect_identical(fs$.__enclos_env__$private$target_schema, "test")
  rm(fs)

})


test_that("DiseasystoreBase$determine_new_ranges works", {

  start_date <- as.Date("2020-01-01")
  end_date   <- as.Date("2020-03-01")
  slice_ts <- glue::glue("{Sys.Date()} 09:00:00")

  conn <- DBI::dbConnect(RSQLite::SQLite())
  fs <- DiseasystoreBase$new(source_conn = "", target_conn = conn)
  logs <- mg_create_logs_if_missing("test.logs", conn)
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
