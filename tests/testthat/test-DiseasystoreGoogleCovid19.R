# Set testing options
withr::local_options("diseasystore.DiseasystoreGoogleCovid19.target_schema" = target_schema_1)
withr::local_options("diseasystore.DiseasystoreGoogleCovid19.n_max" = 1000)


# Store the current options
remote_conn <- diseasyoption("remote_conn", "DiseasystoreGoogleCovid19")
source_conn <- diseasyoption("source_conn", "DiseasystoreGoogleCovid19")
target_conn <- diseasyoption("target_conn", "DiseasystoreGoogleCovid19")


# The files we need are stored remotely in Google's API
google_files <- c("by-age.csv", "demographics.csv", "index.csv", "weather.csv")
remote_conn <- diseasyoption("remote_conn", "DiseasystoreGoogleCovid19")

# In practice, it is best to make a local copy of the data which is stored in the "vignette_data" folder
# This folder can either be in the package folder (preferred, please create the folder) or in the tempdir()
local_conn <- purrr::detect("test_data", checkmate::test_directory_exists, .default = tempdir())

# Then we download the first n rows of each data set of interest
purrr::discard(google_files, ~ checkmate::test_file_exists(file.path(local_conn, .))) |>
  purrr::walk(\(file) {
    paste0(remote_conn, file) |>
      readr::read_csv(n_max = 1000, show_col_types = FALSE, progress = FALSE) |>
      readr::write_csv(file.path(local_conn, file))
  })

# Check that the files are available after attempting to download
if (purrr::some(google_files, ~ !checkmate::test_file_exists(file.path(local_conn, .)))) {
  data_unavailable <- TRUE
} else {
  data_unavailable <- FALSE
}


test_that("DiseasystoreGoogleCovid19 initialises correctly", {

  # Initialise without start_date and end_date
  ds <- expect_no_error(DiseasystoreGoogleCovid19$new(
    verbose = FALSE,
    target_conn = DBI::dbConnect(RSQLite::SQLite())
  ))

  # Check feature store has been created
  checkmate::expect_class(ds, "DiseasystoreGoogleCovid19")
  expect_identical(ds %.% label, "Google COVID-19")


  # Check all FeatureHandlers have been initialised
  private <- ds$.__enclos_env__$private
  feature_handlers <- purrr::keep(ls(private), ~ startsWith(., "google_covid_19")) |>
    purrr::map(~ purrr::pluck(private, .))

  purrr::walk(feature_handlers, ~ {
    checkmate::expect_class(.x, "FeatureHandler")
    checkmate::expect_function(.x %.% compute)
    checkmate::expect_function(.x %.% get)
    checkmate::expect_function(.x %.% key_join)
  })

  rm(ds)
  invisible(gc())
})


test_that("DiseasystoreGoogleCovid19 works with URL source_conn", {
  testthat::skip_if_not(curl::has_internet())

  # Ensure source is set as the remote
  withr::local_options("diseasystore.DiseasystoreGoogleCovid19.source_conn" = remote_conn)

  ds <- expect_no_error(DiseasystoreGoogleCovid19$new(
    target_conn = DBI::dbConnect(RSQLite::SQLite()),
    start_date = as.Date("2020-03-01"),
    end_date = as.Date("2020-03-01"),
    verbose = FALSE
  ))

  expect_no_error(ds$get_feature("n_hospital"))

  rm(ds)
  invisible(gc())
})


# Ensure source is set as the local directory for remaining tests
withr::local_options("diseasystore.DiseasystoreGoogleCovid19.source_conn" = local_conn)


test_that("DiseasystoreGoogleCovid19 works with directory source_conn", {
  testthat::skip_if(data_unavailable)

  ds <- expect_no_error(DiseasystoreGoogleCovid19$new(
    target_conn = DBI::dbConnect(RSQLite::SQLite()),
    start_date = as.Date("2020-03-01"),
    end_date = as.Date("2020-03-01"),
    verbose = FALSE
  ))

  expect_no_error(ds$get_feature("n_hospital"))

  rm(ds)
  invisible(gc())
})


# As a stop-gap measure for the following tests, we close the connections more often to clear dbplyr_### tables
# and, hopefully, prevent the intermittent errors we have been receiving (see issue 113)

# For this purpose, we here store some properties from the diseasystore
# 1) available_features
# 2) available_observables
# 3) ds_map
ds <- expect_no_error(DiseasystoreGoogleCovid19$new(verbose = FALSE, target_conn = DBI::dbConnect(RSQLite::SQLite())))

available_features <- ds$available_features

available_observables  <- ds$available_features |>
  purrr::keep(~ startsWith(., "n_") | endsWith(., "_temperature"))

ds_map <- ds$ds_map

rm(ds)
invisible(gc())


test_that("DiseasystoreGoogleCovid19 can retrieve features from a fresh state", {
  testthat::skip_if(data_unavailable)

  # Attempt to get features from the feature store
  # then check that they match the expected value from the generators
  start_date <- as.Date("2020-03-01")
  end_date   <- as.Date("2020-03-05")

  purrr::walk2(available_features, ds_map, ~ {

    for (conn in get_test_conns()) {

      # Initialise without start_date and end_date
      ds <- expect_no_error(DiseasystoreGoogleCovid19$new(verbose = FALSE, target_conn = conn))

      feature <- ds$get_feature(.x, start_date = start_date, end_date = end_date) |>
        dplyr::collect()

      feature_checksum <- feature |>
        SCDB::digest_to_checksum() |>
        dplyr::pull("checksum") |>
        sort()

      reference_generator <- purrr::pluck(ds, ".__enclos_env__", "private", .y, "compute")

      reference <- reference_generator(start_date  = start_date,
                                       end_date    = end_date,
                                       slice_ts    = ds %.% slice_ts,
                                       source_conn = ds %.% source_conn) |>
        dplyr::copy_to(ds %.% target_conn, df = _, name = "ds_tmp", overwrite = TRUE) |>
        dplyr::collect()

      reference_checksum <- reference |>
        SCDB::digest_to_checksum() |>
        dplyr::pull("checksum") |>
        sort()

      expect_identical(feature_checksum, reference_checksum)

      rm(ds)
      invisible(gc())
    }
  })
})


test_that("DiseasystoreGoogleCovid19 can extend existing features", {
  testthat::skip_if(data_unavailable)

  start_date <- as.Date("2020-03-01")
  end_date   <- as.Date("2020-03-10")

  # Attempt to get features from the feature store (using different dates)
  # then check that they match the expected value from the generators
  purrr::walk2(available_features, ds_map, ~ {

    for (conn in get_test_conns()) {

      # Initialise without start_date and end_date
      ds <- expect_no_error(DiseasystoreGoogleCovid19$new(verbose = FALSE, target_conn = conn))

      feature <- ds$get_feature(.x, start_date = start_date, end_date = end_date) |>
        dplyr::collect() |>
        SCDB::digest_to_checksum() |>
        dplyr::pull("checksum") |>
        sort()

      reference_generator <- purrr::pluck(ds, ".__enclos_env__", "private", .y, "compute")

      reference <- reference_generator(start_date  = start_date,
                                       end_date    = end_date,
                                       slice_ts    = ds %.% slice_ts,
                                       source_conn = ds %.% source_conn) |>
        dplyr::copy_to(ds %.% target_conn, df = _, name = "ds_tmp", overwrite = TRUE) |>
        dplyr::collect() |>
        SCDB::digest_to_checksum() |>
        dplyr::pull("checksum") |>
        sort()

      expect_identical(feature, reference)

      rm(ds)
      invisible(gc())
    }
  })

})


# Helper function that checks the output of key_joins
key_join_features_tester <- function(output, start_date, end_date) {
  # The output dates should match start and end date
  testthat::expect_equal(min(output$date), start_date)
  testthat::expect_equal(max(output$date), end_date)
}


# Set start and end dates for the rest of the tests
start_date <- as.Date("2020-03-01")
end_date   <- as.Date("2020-03-10")



test_that("DiseasystoreGoogleCovid19 can key_join features", {
  testthat::skip_if(data_unavailable)

  for (observable in available_observables) {
    for (conn in get_test_conns()) {

      # Initialise without start_date and end_date
      ds <- expect_no_error(DiseasystoreGoogleCovid19$new(verbose = FALSE, target_conn = conn))

      # Attempt to perform the possible key_joins
      available_stratifications <- ds$available_features |>
        purrr::discard(~ startsWith(., "n_") | endsWith(., "_temperature"))



      # First check we can aggregate without a stratification
      purrr::walk(observable,
                  ~ expect_no_error(ds$key_join_features(observable = as.character(.x),
                                                         stratification = NULL,
                                                         start_date, end_date)))


      # Then test combinations with non-NULL stratifications
      expand.grid(observable     = observable,
                  stratification = available_stratifications) |>
        purrr::pwalk(~ {
          # This code may fail (gracefully) in some cases. These we catch here
          output <- tryCatch({
            ds$key_join_features(
              observable = as.character(..1),
              stratification = eval(parse(text = glue::glue("rlang::quos({..2})"))),
              start_date, end_date
            )
          },
          warning = function(w) {
            checkmate::expect_character(w$message, pattern = "Observable already stratified by")
            return(NULL)
          },
          error = function(e) {
            expect_identical(
              e$message,
              paste("(At least one) stratification feature does not match observable aggregator. Not implemented yet.")
            )
            return(NULL)
          })

          # If the code does not fail, we test the output
          if (!is.null(output)) {
            key_join_features_tester(dplyr::collect(output), start_date, end_date)
          }
        })

      rm(ds)
      invisible(gc())
    }
  }
})


test_that("DiseasystoreGoogleCovid19 key_join fails gracefully", {
  testthat::skip_if(data_unavailable)

  for (observable in available_observables) {
    for (conn in get_test_conns()) {

      # Initialise without start_date and end_date
      ds <- expect_no_error(DiseasystoreGoogleCovid19$new(verbose = FALSE, target_conn = conn))


      # Attempt to perform the possible key_joins
      available_stratifications <- ds$available_features |>
        purrr::discard(~ startsWith(., "n_") | endsWith(., "_temperature"))


      # Test key_join with malformed inputs
      expand.grid(observable     = observable,
                  stratification = "non_existent_stratification") |>
        purrr::pwalk(~ {
          # This code may fail (gracefully) in some cases. These we catch here
          output <- tryCatch({
            ds$key_join_features(
              observable = as.character(..1),
              stratification = ..2,
              start_date = start_date,
              end_date = end_date
            )
          }, error = function(e) {
            checkmate::expect_character(e$message, pattern = "Must be element of set")
            return(NULL)
          })

          # If the code does not fail, we test the output
          if (!is.null(output)) {
            key_join_features_tester(dplyr::collect(output), start_date, end_date)
          }
        })


      expand.grid(observable     = available_observables,
                  stratification = "test = non_existent_stratification") |>
        purrr::pwalk(~ {
          # This code may fail (gracefully) in some cases. These we catch here
          output <- tryCatch({
            ds$key_join_features(
              observable = as.character(..1),
              stratification = eval(parse(text = glue::glue("rlang::quos({..2})"))),
              start_date = start_date,
              end_date = end_date
            )
          }, error = function(e) {
            checkmate::expect_character(
              e$message,
              pattern = glue::glue("Stratification variable not found. ",
                                   "Available stratification variables are: ",
                                   "{toString(available_stratifications)}")
            )
            return(NULL)
          })

          # If the code does not fail, we test the output
          if (!is.null(output)) {
            key_join_features_tester(dplyr::collect(output), start_date, end_date)
          }
        })


      # Clean up
      rm(ds)
    }
    invisible(gc())
  }
})
