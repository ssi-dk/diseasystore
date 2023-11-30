test_that("to_diseasystore_case works", {

  # Initial tests
  expect_identical(to_diseasystore_case("This is a test"),
                   "DiseasystoreThisIsATest")

  expect_identical(to_diseasystore_case("This is a test with numb3r5"),
                   "DiseasystoreThisIsATestWithNumb3r5")

  expect_identical(to_diseasystore_case("This is a test with numb3r5 and dashes--3"),
                   "DiseasystoreThisIsATestWithNumb3r5AndDashes3")


  # Generic case conversions
  expect_identical(to_diseasystore_case("snake_case"),
                   "DiseasystoreSnakeCase")

  expect_identical(to_diseasystore_case("camelCase"),
                   "DiseasystoreCamelCase")

  expect_identical(to_diseasystore_case("PascalCase"),
                   "DiseasystorePascalCase")


  # Specific tests, for Google COVID 19 diseasystore
  expect_identical(to_diseasystore_case("Google covid-19"),
                   "DiseasystoreGoogleCovid19")

  expect_identical(to_diseasystore_case("Google COVID-19"),
                   "DiseasystoreGoogleCovid19")

  expect_identical(to_diseasystore_case("google covid 19"),
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


test_that("source_conn_path works for directories", {

  # Create some files to test for
  test_dir1 <- tempdir()
  expect_true(dir.exists(test_dir1))

  test_file1 <- basename(tempfile())
  saveRDS(mtcars, file = file.path(test_dir1, test_file1))

  test_file2 <- paste0(test_file1, ".gz")
  saveRDS(mtcars, file = file.path(test_dir1, test_file2))

  test_file3 <- basename(tempfile())
  saveRDS(mtcars, file = file.path(test_dir1, paste0(test_file3, ".gz")))

  test_file4 <- basename(tempfile())

  # test_file1 and test_file2 exists
  expect_true(file.exists(file.path(test_dir1, test_file1)))
  expect_true(file.exists(file.path(test_dir1, test_file2)))

  # .. and they give paths as expected
  expect_identical(source_conn_path(test_dir1, test_file1),
                   file.path(test_dir1, test_file1))
  expect_identical(source_conn_path(test_dir1, test_file2),
                   file.path(test_dir1, test_file2))


  # test_file3 and test_file4 does not exists
  expect_false(file.exists(file.path(test_dir1, test_file3))) # does not exist
  expect_false(file.exists(file.path(test_dir1, test_file4))) # does not exist

  # test_file4 give errors as expected
  expect_error(source_conn_path(test_dir1, test_file4),
               class = "simpleError",
               regexp = "could not be found in")

  # but a test_file3.gz exists so we do get a path
  expect_true(file.exists(file.path(test_dir1, paste0(test_file3, ".gz"))))
  expect_identical(source_conn_path(test_dir1, test_file3),
                   file.path(test_dir1, paste0(test_file3, ".gz")))




  # If the files are not in the folder, all gives errors
  test_dir2 <- stringr::str_replace(test_dir1, basename(test_dir1), tempfile(tmpdir = ""))

  expect_error(source_conn_path(test_dir2, test_file1),
               class = "simpleError",
               regexp = "check_directory_exists")
  expect_error(source_conn_path(test_dir2, test_file2),
               class = "simpleError",
               regexp = "check_directory_exists")
  expect_error(source_conn_path(test_dir2, test_file3),
               class = "simpleError",
               regexp = "check_directory_exists")
  expect_error(source_conn_path(test_dir2, test_file4),
               class = "simpleError",
               regexp = "check_directory_exists")
})


test_that("source_conn_path works for URLs", {

  # Create some files to test for
  test_url1 <- "https://test.com/"
  test_url2 <- "https://test.com"

  test_file <- basename(tempfile())


  # Test the different combinations
  checkmate::expect_character(source_conn_path(test_url1, test_file),
                              pattern = paste0(stringr::str_remove(test_url1, "/$"), "/", test_file))

  checkmate::expect_character(source_conn_path(test_url2, test_file),
                              pattern = paste0(stringr::str_remove(test_url2, "/$"), "/", test_file))

})
