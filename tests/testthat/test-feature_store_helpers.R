test_that("diseasystore_case_definition works", {

  # Initial tests
  expect_identical(diseasystore_case_definition("This is a test"),
                   "DiseasystoreThisIsATest")

  expect_identical(diseasystore_case_definition("This is a test with numb3r5"),
                   "DiseasystoreThisIsATestWithNumb3r5")

  expect_identical(diseasystore_case_definition("This is a test with numb3r5 and dashes--3"),
                   "DiseasystoreThisIsATestWithNumb3r5AndDashes3")


  # Generic case conversions
  expect_identical(diseasystore_case_definition("snake_case"),
                   "DiseasystoreSnakeCase")

  expect_identical(diseasystore_case_definition("camelCase"),
                   "DiseasystoreCamelCase")

  expect_identical(diseasystore_case_definition("PascalCase"),
                   "DiseasystorePascalCase")


  # Specific tests, for Google COVID 19 diseasystore
  expect_identical(diseasystore_case_definition("Google covid-19"),
                   "DiseasystoreGoogleCovid19")

  expect_identical(diseasystore_case_definition("Google COVID-19"),
                   "DiseasystoreGoogleCovid19")

  expect_identical(diseasystore_case_definition("google covid 19"),
                   "DiseasystoreGoogleCovid19")
})


test_that("available_diseasystores works", {

  # We should not find the Base or Generic class
  expect_false("DiseasystoreBase" %in% available_diseasystores())
  expect_false("DiseasystoreGeneric" %in% available_diseasystores())

  # We should find the Google COVID 19 class
  expect_true("DiseasystoreGoogleCovid19" %in% available_diseasystores())

})


test_that("diseasystore_exists works", {

  # We should find the Google COVID 19 class
  expect_true(diseasystore_exists("GoogleCovid19"))
  expect_true(diseasystore_exists("Google Covid-19"))
  expect_true(diseasystore_exists("Google COVID 19"))

})


test_that("get_diseasystore works", {

  # We should find the Google COVID 19 class
  expect_identical(get_diseasystore("GoogleCovid19")$classname,   "DiseasystoreGoogleCovid19")
  expect_identical(get_diseasystore("Google Covid-19")$classname, "DiseasystoreGoogleCovid19")
  expect_identical(get_diseasystore("Google COVID 19")$classname, "DiseasystoreGoogleCovid19")

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
