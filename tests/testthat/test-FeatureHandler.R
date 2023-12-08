test_that("FeatureHandler initializes with correctly formed arguments", {

  # Test different initialization of the feature handler module

  # 1) No inputs
  fh <- expect_no_error(FeatureHandler$new())
  checkmate::expect_class(fh, "FeatureHandler")
  checkmate::assert_function(fh$compute,  args = "...")
  checkmate::assert_function(fh$get,      args = c("target_table", "slice_ts", "target_conn"))
  checkmate::assert_function(fh$key_join, args = "...")
  rm(fh)

  # 2) Correctly formed compute function
  fh <- expect_no_error(FeatureHandler$new(compute = \(start_date, end_date, slice_ts, source_conn) 1))
  checkmate::assert_function(fh$compute,  args = c("start_date", "end_date", "slice_ts", "source_conn"))
  checkmate::assert_function(fh$get,      args = c("target_table", "slice_ts", "target_conn"))
  checkmate::assert_function(fh$key_join, args = "...")
  rm(fh)

  # 3) Correctly formed get function
  fh <- expect_no_error(FeatureHandler$new(get = \(target_table, slice_ts, target_conn) 1))
  checkmate::assert_function(fh$compute,  args = "...")
  checkmate::assert_function(fh$get,      args = c("target_table", "slice_ts", "target_conn"))
  checkmate::assert_function(fh$key_join, args = "...")
  rm(fh)

  # 4) Correctly formed key_join function
  fh <- expect_no_error(FeatureHandler$new(key_join = \(.data, feature) 1))
  checkmate::assert_function(fh$compute,  args = "...")
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
