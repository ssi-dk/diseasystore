utils::globalVariables(c("source_conn_path", "source_conn_github"))


#' Test a given diseasy store
#' @description
#'   This function runs a battery of tests of the given diseasystore.
#'
#'   The supplied diseasystore must be a generator for the diseasystore, not an instance of the diseasystore.
#'
#'   The tests assume that data has been made available locally to run the majority of the tests.
#'   The location of the local data should be configured in the options for "source_conn" of the given
#'   diseasystore before calling test_diseasystore.
#'
#' @param diseasystore_generator (`Diseasystore*`)\cr
#'   The diseasystore R6 class generator to test.
#' @param conn_generator (`function`)\cr
#'   Function that generates a `list`() of connections use as target_conn.
#' @param data_files (`character()`)\cr
#'   List of files that should be available when testing.
#' @param target_schema (`character(1)`)\cr
#'   The data base schema where the tests should be run.
#' @param test_start_date (`Date`)\cr
#'   The earliest date to retrieve data from during tests.
#' @return `r rd_side_effects`
#' @examples
#' \donttest{
#'   test_diseasystore(
#'     DiseasystoreGoogleCovid19,
#'     \() list(DBI::dbConnect(RSQLite::SQLite())),
#'     data_files = c("by-age.csv", "demographics.csv", "index.csv", "weather.csv"),
#'     target_schema = "test_ds"
#'   )
#' }
#' @export
test_diseasystore <- function(diseasystore_generator = NULL, conn_generator = NULL,
                              data_files = NULL, target_schema = "test_ds", test_start_date = NULL) {

  # Determine the class of the diseasystore being tested
  diseasystore_class <- diseasystore_generator$classname

  coll <- checkmate::makeAssertCollection()
  checkmate::assert_class(diseasystore_generator, "R6ClassGenerator", add = coll)
  checkmate::assert_choice(as.character(diseasystore_generator$inherit), "DiseasystoreBase", add = coll)
  checkmate::assert_function(conn_generator, add = coll)
  purrr::walk(conn_generator(), ~ checkmate::assert_multi_class(., c("DBIConnection", "OdbcConnection"), add = coll))
  checkmate::assert_character(data_files, null.ok = TRUE, add = coll)
  checkmate::assert_character(target_schema, add = coll)
  checkmate::assert_date(test_start_date, add = coll)
  checkmate::assert_true(is.null(diseasyoption("remote_conn", diseasystore_class)) || curl::has_internet(), add = coll)
  checkmate::reportAssertions(coll)


  # Store the current options for the connections
  remote_conn <- diseasyoption("remote_conn", diseasystore_class)

  # Check file availability
  # In practice, it is best to make a local copy of the data which is stored in the "vignette_data" folder
  # This folder can either be in the package folder (preferred, please create the folder) or in the tempdir()
  local_conn <- purrr::detect("test_data", checkmate::test_directory_exists, .default = tempdir())

  # Then we download the first n rows of each data set of interest
  remote_data_available <- !is.null(remote_conn) && curl::has_internet() # Assume available

  if (remote_data_available) {

    # Determine the type of source_conn_helper needed for the generator
    if (stringr::str_detect(remote_conn,
                            r"{\b(?:https?|ftp):\/\/[-A-Za-z0-9+&@#\/%?=~_|!:,.;]*[-A-Za-z0-9+&@#\/%=~_|]}")) {
      source_conn_helper <- source_conn_path                                                                            # nolint: object_usage_linter
    } else {
      stop("source_conn_helper could not be determined")
    }


    # Then look for availability of files and download if needed
    for (file in data_files) {
      remote_url <- source_conn_helper(remote_conn, file)

      # If we have the file locally, do not re-download but check it exists
      if (checkmate::test_file_exists(file.path(local_conn, file))) {
        remote_data_available <- !identical(
          class(try(readr::read_csv(remote_url, n_max = 1, show_col_types = FALSE, progress = FALSE))),
          "try-error"
        )
      } else { # If we don't have the file locally, copy it down
        remote_data_available <- !identical(
          class(
            try({
              readr::read_csv(remote_url, n_max = diseasyoption("n_max", diseasystore_class, .default = 1000),
                              show_col_types = FALSE, progress = FALSE) |>
                readr::write_csv(file.path(local_conn, file))
            })
          ),
          "try-error"
        )
      }
    }
  }


  # Throw warning if data unavailable
  if (!is.null(remote_conn) && curl::has_internet() && !remote_data_available) {
    warning(glue::glue("remote_conn for {diseasystore_class} unavailable despite internet being available!"))
  }

  # Check that the files are available after attempting to download
  local <- purrr::every(data_files, ~ checkmate::test_file_exists(file.path(local_conn, .)))


  # Set testing options
  # - set the target_schema for the diseasystore during tests
  # - limit the number of records to use in tests
  # - Use the local test data directory for testing
  withr::local_options(stats::setNames(target_schema, glue::glue("diseasystore.{diseasystore_class}.target_schema")))
  withr::local_options(stats::setNames(local_conn,    glue::glue("diseasystore.{diseasystore_class}.source_conn")))
  withr::local_options(stats::setNames(1000,          glue::glue("diseasystore.{diseasystore_class}.n_max")))




  #     ######## ########  ######  ########  ######     ########  ########  ######   #### ##    ##  ######
  #        ##    ##       ##    ##    ##    ##    ##    ##     ## ##       ##    ##   ##  ###   ## ##    ##
  #        ##    ##       ##          ##    ##          ##     ## ##       ##         ##  ####  ## ##
  #        ##    ######    ######     ##     ######     ########  ######   ##   ####  ##  ## ## ##  ######
  #        ##    ##             ##    ##          ##    ##     ## ##       ##    ##   ##  ##  ####       ##
  #        ##    ##       ##    ##    ##    ##    ##    ##     ## ##       ##    ##   ##  ##   ### ##    ##
  #        ##    ########  ######     ##     ######     ########  ########  ######   #### ##    ##  ######

  testthat::test_that(glue::glue("{diseasystore_class} initialises correctly"), {

    # Initialise without start_date and end_date
    ds <- testthat::expect_no_error(diseasystore_generator$new(
      verbose = FALSE,
      target_conn = DBI::dbConnect(RSQLite::SQLite())
    ))

    # Check feature store has been created
    checkmate::expect_class(ds, diseasystore_class)

    # Check all FeatureHandlers have been initialised
    feature_handlers <- diseasystore_generator$private_fields$.ds_map |>
      purrr::map(~ purrr::pluck(diseasystore_generator$private_fields, .))

    purrr::walk(feature_handlers, ~ {
      checkmate::expect_class(.x, "FeatureHandler")
      checkmate::expect_function(.x %.% compute)
      checkmate::expect_function(.x %.% get)
      checkmate::expect_function(.x %.% key_join)
    })

    rm(ds)
    invisible(gc())
  })


  testthat::test_that(glue::glue("{diseasystore_class} can initialise with remote source_conn"), {
    testthat::skip_if_not(curl::has_internet())
    testthat::skip_if_not(remote_data_available)

    # Ensure source is set as the remote
    withr::local_options(
      stats::setNames(
        diseasyoption("remote_conn", diseasystore_class),
        glue::glue("diseasystore.{diseasystore_class}.source_conn")
      )
    )

    ds <- testthat::expect_no_error(diseasystore_generator$new(
      target_conn = DBI::dbConnect(RSQLite::SQLite()),
      start_date = test_start_date,
      end_date = test_start_date,
      verbose = FALSE
    ))

    feature <- testthat::expect_no_error(ds$available_features[[1]])
    testthat::expect_no_error(ds$get_feature(feature))

    rm(ds)
    invisible(gc())
  })


  testthat::test_that(glue::glue("{diseasystore_class} can initialise with default source_conn"), {
    testthat::skip_if_not_installed("RSQLite")
    testthat::skip_if_not(local)

    ds <- testthat::expect_no_error(diseasystore_generator$new(
      target_conn = DBI::dbConnect(RSQLite::SQLite()),
      start_date = test_start_date,
      end_date = test_start_date,
      verbose = FALSE
    ))

    testthat::expect_no_error(ds$get_feature(ds$available_features[[1]]))

    rm(ds)
    invisible(gc())
  })


  testthat::test_that(glue::glue("{diseasystore_class} can retrieve features from a fresh state"), {
    testthat::skip_if_not(local)

    for (conn in conn_generator()) {

      # Initialise without start_date and end_date
      ds <- testthat::expect_no_error(diseasystore_generator$new(verbose = FALSE, target_conn = conn))

      # Attempt to get features from the feature store
      # then check that they match the expected value from the generators
      purrr::walk2(ds$available_features, ds$ds_map, ~ {
        start_date <- test_start_date
        end_date   <- test_start_date + lubridate::days(4)

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

        testthat::expect_identical(feature_checksum, reference_checksum)

        # Stop-gap measure to clear dbplyr_### tables
        SCDB::get_tables(ds %.% target_conn, show_temp = TRUE) |>
          dplyr::pull(table) |>
          purrr::keep(~ stringr::str_detect(., "#?dbplyr_")) |>
          purrr::walk(~ DBI::dbRemoveTable(ds %.% target_conn, .))
      })

      rm(ds)
      invisible(gc())
    }
  })


  testthat::test_that(glue::glue("{diseasystore_class} can extend existing features"), {
    testthat::skip_if_not(local)

    for (conn in conn_generator()) {

      # Initialise without start_date and end_date
      ds <- testthat::expect_no_error(diseasystore_generator$new(verbose = FALSE, target_conn = conn))

      # Attempt to get features from the feature store (using different dates)
      # then check that they match the expected value from the generators
      purrr::walk2(ds$available_features, ds$ds_map, ~ {
        start_date <- test_start_date
        end_date   <- test_start_date + lubridate::days(9)

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

        testthat::expect_identical(feature, reference)

        # Stop-gap measure to clear dbplyr_### tables
        SCDB::get_tables(ds %.% target_conn, show_temp = TRUE) |>
          dplyr::pull(table) |>
          purrr::keep(~ stringr::str_detect(., "#?dbplyr_")) |>
          purrr::walk(~ DBI::dbRemoveTable(ds %.% target_conn, .))
      })

      rm(ds)
      invisible(gc())
    }
  })


  # Helper function that checks the output of key_joins
  key_join_features_tester <- function(output, start_date, end_date) {                                                  # nolint: object_usage_linter
    # The output dates should match start and end date
    testthat::expect_equal(min(output$date), start_date)
    testthat::expect_equal(max(output$date), end_date)
  }


  # Set start and end dates for the rest of the tests
  start_date <- test_start_date                                                                                         # nolint: object_usage_linter
  end_date   <- test_start_date + lubridate::days(9)                                                                    # nolint: object_usage_linter



  testthat::test_that(glue::glue("{diseasystore_class} can key_join features"), {
    testthat::skip_if_not(local)

    for (conn in conn_generator()) {

      # Initialise without start_date and end_date
      ds <- testthat::expect_no_error(diseasystore_generator$new(verbose = FALSE, target_conn = conn))

      # Attempt to perform the possible key_joins
      available_observables  <- ds$available_features |>
        purrr::keep(~ startsWith(., "n_") | endsWith(., "_temperature"))
      available_stratifications <- ds$available_features |>
        purrr::discard(~ startsWith(., "n_") | endsWith(., "_temperature"))



      # First check we can aggregate without a stratification
      for (observable in available_observables) {
        testthat::expect_no_error(
          ds$key_join_features(observable = observable, stratification = NULL, start_date, end_date)
        )
      }



      # Then test combinations with non-NULL stratifications
      expand.grid(observable     = available_observables,
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
          error = function(e) {
            testthat::expect_identical(
              e$message,
              paste("(At least one) stratification feature does not match observable aggregator. Not implemented yet.")
            )
            return(NULL)
          },
          warning = function(w) {
            checkmate::expect_character(w$message, pattern = "Observable already stratified by")
            return(NULL)
          })

          # If the code does not fail, we test the output
          if (!is.null(output)) {
            key_join_features_tester(dplyr::collect(output), start_date, end_date)
          }

          # Stop-gap measure to clear dbplyr_### tables
          SCDB::get_tables(ds %.% target_conn, show_temp = TRUE) |>
            dplyr::pull(table) |>
            purrr::keep(~ stringr::str_detect(., "#?dbplyr_")) |>
            purrr::walk(~ DBI::dbRemoveTable(ds %.% target_conn, .))
        })

      rm(ds)
      invisible(gc())
    }
  })


  testthat::test_that(glue::glue("{diseasystore_class} key_join fails gracefully"), {
    testthat::skip_if_not(local)

    for (conn in conn_generator()) {

      # Initialise without start_date and end_date
      ds <- testthat::expect_no_error(diseasystore_generator$new(verbose = FALSE, target_conn = conn))


      # Attempt to perform the possible key_joins
      available_observables  <- ds$available_features |>
        purrr::keep(~ startsWith(., "n_") | endsWith(., "_temperature"))
      available_stratifications <- ds$available_features |>
        purrr::discard(~ startsWith(., "n_") | endsWith(., "_temperature"))


      # Test key_join with malformed inputs
      expand.grid(observable     = available_observables,
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

          # Stop-gap measure to clear dbplyr_### tables
          SCDB::get_tables(ds %.% target_conn, show_temp = TRUE) |>
            dplyr::pull(table) |>
            purrr::keep(~ stringr::str_detect(., "#?dbplyr_")) |>
            purrr::walk(~ DBI::dbRemoveTable(ds %.% target_conn, .))
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

          # Stop-gap measure to clear dbplyr_### tables
          SCDB::get_tables(ds %.% target_conn, show_temp = TRUE) |>
            dplyr::pull(table) |>
            purrr::keep(~ stringr::str_detect(., "#?dbplyr_")) |>
            purrr::walk(~ DBI::dbRemoveTable(ds %.% target_conn, .))
        })

      # Clean up
      rm(ds)
      invisible(gc())
    }
  })
}
