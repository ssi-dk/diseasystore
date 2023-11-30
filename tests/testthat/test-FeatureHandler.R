test_that("FeatureHandler initializes with correctly formed arguments", {

  # Test different initialization of the feature handler module

  # 1) No inputs
  expect_no_error(fh <- FeatureHandler$new())
  checkmate::expect_class(fh, "FeatureHandler")
  checkmate::assert_function(fh$compute,  args = c("..."))
  checkmate::assert_function(fh$get,      args = c("target_table", "slice_ts", "target_conn"))
  checkmate::assert_function(fh$key_join, args = c("..."))
  rm(fh)

  # 2) Correctly formed compute function
  expect_no_error(fh <- FeatureHandler$new(compute = \(start_date, end_date, slice_ts, source_conn) 1))
  checkmate::assert_function(fh$compute,  args = c("start_date", "end_date", "slice_ts", "source_conn"))
  checkmate::assert_function(fh$get,      args = c("target_table", "slice_ts", "target_conn"))
  checkmate::assert_function(fh$key_join, args = c("..."))
  rm(fh)

  # 3) Correctly formed get function
  expect_no_error(fh <- FeatureHandler$new(get = \(target_table, slice_ts, target_conn) 1))
  checkmate::assert_function(fh$compute,  args = c("..."))
  checkmate::assert_function(fh$get,      args = c("target_table", "slice_ts", "target_conn"))
  checkmate::assert_function(fh$key_join, args = c("..."))
  rm(fh)

  # 4) Correctly formed key_join function
  expect_no_error(fh <- FeatureHandler$new(key_join = \(.data, feature) 1))
  checkmate::assert_function(fh$compute,  args = c("..."))
  checkmate::assert_function(fh$get,      args = c("target_table", "slice_ts", "target_conn"))
  checkmate::assert_function(fh$key_join, args = c(".data", "feature"))
  rm(fh)

})


test_that("FeatureHandler fails gracefully with malformed arguments", {

  # Test different initialization of the feature handler module

  # 1) Malformed compute function
  expect_error(fh <- FeatureHandler$new(compute = \(start_date, end_date, slice_ts) 1),
               regexp = "source_conn")

  # 2) Malformed get function
  expect_error(fh <- FeatureHandler$new(get = \(target_table, slice_ts) 1),
               regexp = "target_conn")

  # 3) Malformed key_join function
  expect_error(fh <- FeatureHandler$new(key_join = \(.data) 1),
               regexp = "feature")

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
