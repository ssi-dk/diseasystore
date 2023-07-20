test_that("DiseasystoreGoogleCovid19 works", {

  # First we create a temporary directory for the Google COVID-19 data
  remote_conn <- options() %.% diseasystore.DiseasystoreGoogleCovid19.remote_conn
  tmp_dir <- tempdir()
  options(diseasystore.DiseasystoreGoogleCovid19.source_conn = tmp_dir)

  sqlite_path <- file.path(tmp_dir, "diseasystore_google_covid_19.sqlite")
  if (file.exists(sqlite_path)) {
    closeAllConnections()
    file.remove(sqlite_path)
  }
  target_conn <- \() dbConnect(RSQLite::SQLite(), sqlite_path)
  options(diseasystore.DiseasystoreGoogleCovid19.target_conn = target_conn)


  # Then we download the first n rows of each data set of interest
  google_files <- c("by-age.csv", "demographics.csv", "index.csv", "weather.csv")
  purrr::walk(google_files, ~ {
    readr::read_csv(paste0(remote_conn, .), n_max = 1000, show_col_types = FALSE, progress = FALSE) |>
    readr::write_csv(file.path(tmp_dir, .))
  })

  start_date <- as.Date("2020-03-01")
  end_date   <- as.Date("2020-12-31")
  fs <- DiseasystoreGoogleCovid19$new(start_date = start_date,
                                      end_date   = end_date,
                                      verbose = FALSE)

  # Check feature store has been created
  expect_class(fs, "DiseasystoreGoogleCovid19")

  # Check all FeatureHandlers have been initialized
  private <- fs$.__enclos_env__$private
  feature_handlers <- purrr::keep(ls(private), ~ startsWith(., "google_covid_19")) |>
    purrr::map(~ purrr::pluck(private, .))

  purrr::walk(feature_handlers,
    ~ {
        expect_class(.x, "FeatureHandler")
        checkmate::expect_function(.x %.% compute)
        checkmate::expect_function(.x %.% get)
        checkmate::expect_function(.x %.% key_join)
    })

  # Attempt to get features from the feature store
  # then check that they match the expected value from the generators
  purrr::walk2(fs$available_features, names(fs$fs_map),
    ~ {
        feature <- fs$get_feature(.x, start_date = start_date, end_date = end_date) |>
          dplyr::collect()

        feature_checksum <- feature |>
          mg_digest_to_checksum() |>
          dplyr::pull("checksum") |>
          sort()

        reference_generator <- eval(parse(text = paste0(.y, "_()"))) %.% compute

        reference <- reference_generator(start_date  = start_date,
                                         end_date    = end_date,
                                         slice_ts    = fs$.__enclos_env__$private$slice_ts,
                                         source_conn = fs$.__enclos_env__$private$source_conn) %>%
          dplyr::copy_to(fs$.__enclos_env__$private$target_conn, ., name = "fs_tmp", overwrite = TRUE) |>
          dplyr::collect()

        reference_checksum <- reference |>
          mg_digest_to_checksum() |>
          dplyr::pull("checksum") |>
          sort()

        expect_identical(feature_checksum, reference_checksum)
    })


  # Attempt to get features from the feature store (using different dates)
  # then check that they match the expected value from the generators
  start_date <- as.Date("2020-04-01")
  end_date   <- as.Date("2020-11-30")
  purrr::walk2(fs$available_features, names(fs$fs_map),
   ~ {
       feature <- fs$get_feature(.x, start_date = start_date, end_date = end_date) |>
         dplyr::collect() |>
         mg_digest_to_checksum() |>
         dplyr::pull("checksum") |>
         sort()

       reference_generator <- eval(parse(text = paste0(.y, "_()"))) %.% compute

       reference <- reference_generator(start_date  = start_date,
                                        end_date    = end_date,
                                        slice_ts    = fs$.__enclos_env__$private$slice_ts,
                                        source_conn = fs$.__enclos_env__$private$source_conn) %>%
         dplyr::copy_to(fs$.__enclos_env__$private$target_conn, ., name = "fs_tmp", overwrite = TRUE) |>
         dplyr::collect() |>
         mg_digest_to_checksum() |>
         dplyr::pull("checksum") |>
         sort()

       expect_identical(feature, reference)
     })

  # Cleanup
  rm(fs)
})