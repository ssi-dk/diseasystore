test_that("DiseasystoreGoogleCovid19 works", {

  # First we create a temporary directory for the Google COVID-19 data
  remote_conn <- options() %.% diseasystore.DiseasystoreGoogleCovid19.remote_conn

  tmp_dir <- stringr::str_replace_all(tempdir(), r"{\\}", .Platform$file.sep)
  options(diseasystore.DiseasystoreGoogleCovid19.source_conn = tmp_dir)

  sqlite_path <- file.path(tmp_dir, "diseasystore_google_covid_19.sqlite")
  if (file.exists(sqlite_path)) {
    closeAllConnections()
    stopifnot("Could not delete SQLite DB before tests" = file.remove(sqlite_path))
  }

  target_conn <- \() dbConnect(RSQLite::SQLite(), sqlite_path)
  options(diseasystore.DiseasystoreGoogleCovid19.target_conn = target_conn)


  # Then we download the first n rows of each data set of interest
  google_files <- c("by-age.csv", "demographics.csv", "index.csv", "weather.csv")
  purrr::walk(google_files, ~ {
      readr::read_csv(paste0(remote_conn, .), n_max = 1000, show_col_types = FALSE, progress = FALSE) |> # nolint: indentation_linter
      readr::write_csv(file.path(tmp_dir, .))
  })

  # Initialize without start_date and end_date
  expect_no_error(fs <- DiseasystoreGoogleCovid19$new(verbose = FALSE))

  # Check feature store has been created
  checkmate::expect_class(fs, "DiseasystoreGoogleCovid19")
  expect_equal(fs %.% case_definition, "Google COVID-19")


  # SCDB v0.1 gives a warning if database has no tables when used. We suppress this warning here
  # if this warning is no longer cast, remove this test
  expect_warning(SCDB::get_tables(fs$target_conn),
                 "No tables found. Check user privileges / database configuration")
  dplyr::copy_to(fs$target_conn, mtcars)
  expect_no_warning(SCDB::get_tables(fs$target_conn))


  # Check all FeatureHandlers have been initialized
  private <- fs$.__enclos_env__$private
  feature_handlers <- purrr::keep(ls(private), ~ startsWith(., "google_covid_19")) |>
    purrr::map(~ purrr::pluck(private, .))

  purrr::walk(feature_handlers, ~ {
    checkmate::expect_class(.x, "FeatureHandler")
    checkmate::expect_function(.x %.% compute)
    checkmate::expect_function(.x %.% get)
    checkmate::expect_function(.x %.% key_join)
  })

  # Attempt to get features from the feature store
  # then check that they match the expected value from the generators
  purrr::walk2(fs$available_features, names(fs$fs_map), ~ {
    start_date <- as.Date("2020-03-01")
    end_date   <- as.Date("2020-12-31")

    feature <- fs$get_feature(.x, start_date = start_date, end_date = end_date) |>
      dplyr::collect()

    feature_checksum <- feature |>
      SCDB::digest_to_checksum() |>
      dplyr::pull("checksum") |>
      sort()

    reference_generator <- eval(parse(text = paste0(.y, "_()"))) %.% compute

    reference <- reference_generator(start_date  = start_date,
                                     end_date    = end_date,
                                     slice_ts    = fs %.% slice_ts,
                                     source_conn = fs %.% source_conn) %>%
      dplyr::copy_to(fs %.% target_conn, ., overwrite = TRUE) |>
      dplyr::collect()

    reference_checksum <- reference |>
      SCDB::digest_to_checksum() |>
      dplyr::pull("checksum") |>
      sort()

    expect_identical(feature_checksum, reference_checksum)
  })


  # Attempt to get features from the feature store (using different dates)
  # then check that they match the expected value from the generators
  purrr::walk2(fs$available_features, names(fs$fs_map), ~ {
    start_date <- as.Date("2020-04-01")
    end_date   <- as.Date("2020-11-30")

    feature <- fs$get_feature(.x, start_date = start_date, end_date = end_date) |>
      dplyr::collect() |>
      SCDB::digest_to_checksum() |>
      dplyr::pull("checksum") |>
      sort()

    reference_generator <- eval(parse(text = paste0(.y, "_()"))) %.% compute

    reference <- reference_generator(start_date  = start_date,
                                     end_date    = end_date,
                                     slice_ts    = fs %.% slice_ts,
                                     source_conn = fs %.% source_conn) %>%
      dplyr::copy_to(fs %.% target_conn, ., name = "fs_tmp", overwrite = TRUE) |>
      dplyr::collect() |>
      SCDB::digest_to_checksum() |>
      dplyr::pull("checksum") |>
      sort()

    expect_identical(feature, reference)
  })

  # Attempt to perform the possible key_joins
  available_observables  <- purrr::keep(fs$available_features,    ~ startsWith(., "n_"))
  available_aggregations <- purrr::discard(fs$available_features, ~ startsWith(., "n_"))


  key_join_features_tester <- function(output, start_date, end_date) {
    # The output dates should match start and end date
    expect_true(min(output$date) == start_date)
    expect_true(max(output$date) == end_date)
  }

  # Set start and end dates for the rest of the tests
  start_date <- as.Date("2020-03-01")
  end_date   <- as.Date("2020-12-31")

  # First check we can aggregate without an aggregation
  purrr::walk(available_observables,
              ~ expect_no_error(fs$key_join_features(observable = as.character(.),
                                                     aggregation = NULL,
                                                     start_date, end_date)))

  # Then test combinations with non-NULL aggregations
  expand.grid(observable  = available_observables,
              aggregation = available_aggregations) |>
    purrr::pwalk(~ {
      # This code may fail (gracefully) in some cases. These we catch here
      output <- tryCatch({
        fs$key_join_features(observable = as.character(..1),
                             aggregation = eval(parse(text = glue::glue("rlang::quos({..2})"))),
                             start_date, end_date)
      }, error = function(e) {
        expect_equal(e$message, paste("(At least one) aggregation feature does not match observable aggregator.",
                                      "Not implemented yet."))
        return(NULL)
      })

      # If the code does not fail, we test the output
      if (!is.null(output)) {
        key_join_features_tester(dplyr::collect(output), start_date, end_date)
      }
    })

  # Cleanup
  rm(fs)
})
