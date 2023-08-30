test_that("diseasyoptoion works", {

  # Store the current options
  opt1 <- options("diseasystore.target_schema")
  opt2 <- options("diseasystore.DiseasystoreGoogleCovid19.target_schema")

  # Set them to default (empty)
  options("diseasystore.target_schema" = "")
  options("diseasystore.DiseasystoreGoogleCovid19.target_schema" = "")

  # Check that diseasyoption works for default values
  expect_equal(diseasyoption("target_schema"), NULL)

  options("diseasystore.target_schema" = "target_schema_1")
  expect_equal(diseasyoption("target_schema"), "target_schema_1")

  # Check that it works for child classes
  ds <- DiseasystoreGoogleCovid19$new(target_conn = DBI::dbConnect(RSQLite::SQLite()))
  expect_equal(diseasyoption("target_schema", "DiseasystoreGoogleCovid19"), "target_schema_1")
  expect_equal(diseasyoption("target_schema", ds), "target_schema_1")

  options("diseasystore.DiseasystoreGoogleCovid19.target_schema" = "target_schema_2")
  expect_equal(diseasyoption("target_schema", "DiseasystoreGoogleCovid19"), "target_schema_2")
  expect_equal(diseasyoption("target_schema", ds), "target_schema_2")

  # Reset options
  options("diseasystore.target_schema" = opt1)
  options("diseasystore.DiseasystoreGoogleCovid19.target_schema" = opt2)
})


test_that("%.% works", {

  d <- list(a = 2, b = 3, .c = 4)

  expect_identical(d %.% a, 2)
  expect_identical(d %.% b, 3)
  expect_identical(d %.% .c, 4)
  expect_error(d %.% d, "d not found in d")


  d <- c(a = 2, b = 3, .c = 4)

  expect_identical(d %.% a, 2)
  expect_identical(d %.% b, 3)
  expect_identical(d %.% .c, 4)
  expect_error(d %.% d, "d not found in d")


  d <- R6::R6Class(public = list(a = 2, b = 3, .c = 4))$new()

  expect_identical(d %.% a, 2)
  expect_identical(d %.% b, 3)
  expect_identical(d %.% .c, 4)
  expect_error(d %.% d, "d not found in d")
})
