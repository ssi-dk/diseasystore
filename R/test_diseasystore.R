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
#'   Should take a `skip_backend` that does not open connections for the given backends.
#' @param data_files (`character()`)\cr
#'   List of files that should be available when testing.
#' @param target_schema `r rd_target_schema()`
#' @param test_start_date (`Date`)\cr
#'   The earliest date to retrieve data from during tests.
#' @param skip_backends (`character()`)\cr
#'   List of connection types to skip tests for due to missing functionality.
#' @param ...
#'   Other parameters passed to the diseasystore generator.
#' @return `r rd_side_effects`
#' @examplesIf requireNamespace("RSQLite", quietly = TRUE)
#' \donttest{
#'   withr::local_options("diseasystore.DiseasystoreEcdcRespiratoryViruses.pull" = FALSE)
#'
#'   conn_generator <- function(skip_backends = NULL) {
#'      switch(
#'        ("SQLiteConnection" %in% skip_backends) + 1,
#'        list(DBI::dbConnect(RSQLite::SQLite())), # SQLiteConnection not in skip_backends
#'        list() # SQLiteConnection in skip_backends
#'      )
#'   }
#'
#'   test_diseasystore(
#'     DiseasystoreEcdcRespiratoryViruses,
#'     conn_generator,
#'     data_files = "data/snapshots/2023-11-24_ILIARIRates.csv",
#'     target_schema = "test_ds",
#'     test_start_date = as.Date("2022-06-20"),
#'     slice_ts = "2023-11-24"
#'   )
#' }
#' @importFrom curl has_internet
#' @export
test_diseasystore <- function(
  diseasystore_generator = NULL,
  conn_generator = NULL,
  data_files = NULL,
  target_schema = "test_ds",
  test_start_date = NULL,
  skip_backends = NULL,
  ...
) {

  # Determine the class of the diseasystore being tested
  diseasystore_class <- diseasystore_generator$classname

  coll <- checkmate::makeAssertCollection()
  checkmate::assert_class(diseasystore_generator, "R6ClassGenerator", add = coll)
  checkmate::assert_function(conn_generator, args = "skip_backends", add = coll)
  conns <- conn_generator()
  purrr::walk(conns, ~ checkmate::assert_multi_class(., c("DBIConnection", "OdbcConnection"), add = coll))
  purrr::walk(conns, DBI::dbDisconnect)
  checkmate::assert_character(data_files, null.ok = TRUE, add = coll)
  checkmate::assert_character(target_schema, add = coll)
  checkmate::assert_date(test_start_date, add = coll)
  checkmate::assert_character(skip_backends, null.ok = TRUE, add = coll)
  checkmate::assert_true(is.null(diseasyoption("remote_conn", diseasystore_class)) || curl::has_internet(), add = coll)
  checkmate::reportAssertions(coll)


  # Store the current options for the connections
  remote_conn <- diseasyoption("remote_conn", diseasystore_class)

  # Check file availability
  # In practice, it is best to make a local copy of the data which is stored in the "test_data" folder
  # This folder can either be in the package folder (preferred, please create the folder) or in the tempdir()
  local_conn <- purrr::detect("test_data", checkmate::test_directory_exists, .default = tempdir())

  # Then we download the first n rows of each data set of interest
  remote_data_available <- !is.null(remote_conn) && curl::has_internet() # Assume available

  if (remote_data_available) {
    # Assume we can run on CRAN
    skip_on_cran <- FALSE

    # Determine the type of source_conn_helper needed for the generator
    if (stringr::str_detect(remote_conn, r"{https?:\/\/api.github.com\/repos\/[a-zA-Z-]*\/[a-zA-Z-]*}")) {
      source_conn_helper <- source_conn_github                                                                          # nolint: object_usage_linter
      skip_on_cran <- TRUE # CRAN tests cannot use authenticated API requests to Github and will fail
    } else if (stringr::str_detect(remote_conn,
                                   r"{\b(?:https?|ftp):\/\/[-A-Za-z0-9+&@#\/%?=~_|!:,.;]*[-A-Za-z0-9+&@#\/%=~_|]}")) {
      source_conn_helper <- source_conn_path                                                                            # nolint: object_usage_linter
    } else {
      stop("`source_conn_helper` could not be determined!", call. = FALSE)
    }

    # Then look for availability of files and download if needed
    on_cran <- !interactive() && !identical(Sys.getenv("NOT_CRAN"), "true") && !identical(Sys.getenv("CI"), "true")

    if (on_cran && skip_on_cran) {

      remote_data_available <- FALSE

    } else {

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
                dir.create(file.path(local_conn, dirname(file)), recursive = TRUE, showWarnings = FALSE)
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
  }


  # Throw warning if remote data unavailable (only throw if we are working locally, don't run this check on CRAN)
  if (!is.null(remote_conn) && curl::has_internet() && !on_cran && !remote_data_available) {
    warning(
      glue::glue("remote_conn for {diseasystore_class} unavailable despite internet being available!"),
      call. = FALSE
    )
  }

  # Check that the files are available after attempting to download
  local <- purrr::every(data_files, ~ checkmate::test_file_exists(file.path(local_conn, .)))


  # Set testing options
  # - set the target_schema for the diseasystore during tests
  # - limit the number of records to use in tests
  # - Use the local test data directory for testing
  # - Reduce the lock wait during test in case of deadlocks by failed test (1 minute during tests)
  withr::local_options(stats::setNames(target_schema, glue::glue("diseasystore.{diseasystore_class}.target_schema")))
  withr::local_options(stats::setNames(local_conn,    glue::glue("diseasystore.{diseasystore_class}.source_conn")))
  withr::local_options(stats::setNames(1000,          glue::glue("diseasystore.{diseasystore_class}.n_max")))
  withr::local_options(stats::setNames(60,            glue::glue("diseasystore.{diseasystore_class}.lock_wait_max")))



  # Make a little helper function to clean up the connection after each test
  # and check that intermediate dbplyr and SCDB tables (e.g. from dplyr::compute() calls) are cleaned up
  connection_clean_up <- function(conn) {
    testthat::expect_identical(nrow(SCDB::get_tables(conn, show_temporary = TRUE, pattern = "dbplyr_")), 0L)
    testthat::expect_identical(nrow(SCDB::get_tables(conn, show_temporary = TRUE, pattern = "SCDB_")), 0L)
    DBI::dbDisconnect(conn)
  }

  #     ######## ########  ######  ########  ######     ########  ########  ######   #### ##    ##  ######
  #        ##    ##       ##    ##    ##    ##    ##    ##     ## ##       ##    ##   ##  ###   ## ##    ##
  #        ##    ##       ##          ##    ##          ##     ## ##       ##         ##  ####  ## ##
  #        ##    ######    ######     ##     ######     ########  ######   ##   ####  ##  ## ## ##  ######
  #        ##    ##             ##    ##          ##    ##     ## ##       ##    ##   ##  ##  ####       ##
  #        ##    ##       ##    ##    ##    ##    ##    ##     ## ##       ##    ##   ##  ##   ### ##    ##
  #        ##    ########  ######     ##     ######     ########  ########  ######   #### ##    ##  ######

  testthat::test_that(glue::glue("{diseasystore_class} initialises correctly"), {
    testthat::skip_if_not_installed("RSQLite")
    testthat::skip_if("SQLiteConnection" %in% skip_backends)

    # Initialise without start_date and end_date
    ds <- testthat::expect_no_error(diseasystore_generator$new(
      verbose = FALSE,
      target_conn = DBI::dbConnect(RSQLite::SQLite()),
      ...
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

    # Check that the min and max dates have been set
    checkmate::expect_date(ds$min_start_date, upper = lubridate::today())
    checkmate::expect_date(ds$max_end_date,   upper = lubridate::today())

    # Clean up
    rm(ds)
    invisible(gc())
  })


  testthat::test_that(glue::glue("{diseasystore_class} can initialise with remote source_conn"), {
    testthat::skip_if_not_installed("RSQLite")
    testthat::skip_if("SQLiteConnection" %in% skip_backends)
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
      verbose = FALSE,
      ...
    ))

    feature <- testthat::expect_no_error(ds$available_features[[1]])
    testthat::expect_no_error(ds$get_feature(feature))

    # Clean up
    rm(ds)
    invisible(gc())
  })


  testthat::test_that(glue::glue("{diseasystore_class} can initialise with default source_conn"), {
    testthat::skip_if_not_installed("RSQLite")
    testthat::skip_if("SQLiteConnection" %in% skip_backends)
    testthat::skip_if_not(local)

    ds <- testthat::expect_no_error(diseasystore_generator$new(
      target_conn = DBI::dbConnect(RSQLite::SQLite()),
      start_date = test_start_date,
      end_date = test_start_date,
      verbose = FALSE,
      ...
    ))

    testthat::expect_no_error(ds$get_feature(ds$available_features[[1]]))

    # Clean up
    rm(ds)
    invisible(gc())
  })


  testthat::test_that(glue::glue("Skipped test connection are also disallowed in {diseasystore_class} constructor "), {
    testthat::skip_if(is.null(skip_backends))

    # Check that the constructor throws an error if the connection is skipped in the tests
    for (conn in conn_generator()) {
      if (!checkmate::test_multi_class(conn, skip_backends)) {
        DBI::dbDisconnect(conn)
        next
      }

      # Check that an error is thrown when attempting to use the connection
      # This error is a checkmate assertion, so we remove newlines and `*` from the formatted error message
      testthat::expect_error(
        tryCatch(
          diseasystore_generator$new(verbose = FALSE, target_conn = conn),
          error = \(e) {
            e$message |>
              stringr::str_remove_all(stringr::fixed("\n *")) |>
              stringr::str_remove_all(stringr::fixed("* ")) |>
              simpleError(message = _) |>
              stop()                                                                                                    # nolint: condition_call_linter
          }
        ),
        paste0("Must be disjunct from \\{'", paste(skip_backends, collapse = "|"), "\\'}")
      )

    }
  })


  # Set a test_end_date for the test
  test_end_date <- test_start_date + lubridate::days(4)


  testthat::test_that(glue::glue("{diseasystore_class} can retrieve features from a fresh state"), {
    testthat::skip_if_not(local)

    for (conn in conn_generator(skip_backends)) {

      # Initialise without start_date and end_date
      ds <- testthat::expect_no_error(diseasystore_generator$new(verbose = FALSE, target_conn = conn, ...))

      # Attempt to get features from the feature store
      # then check that they match the expected value from the generators
      purrr::walk2(ds$available_features, ds$ds_map, ~ {

        # Suppress our user facing, informative warnings
        pkgcond::suppress_warnings(
          class = c(
            "diseasystore::age_on_date.SQLiteConnection-warning",
            "diseasystore::add_years.SQLiteConnection-warning"
          ),
          expr = {
            feature_checksums <- ds$get_feature(.x, start_date = test_start_date, end_date = test_end_date) |>
              SCDB::digest_to_checksum()

            # digest_to_checksum() creates an intermediary table in SQLite
            if (inherits(conn, "SQLiteConnection")) SCDB::defer_db_cleanup(feature_checksums)

            feature_checksums <- feature_checksums |>
              dplyr::pull("checksum") |>
              sort()


            reference_generator <- purrr::pluck(ds, ".__enclos_env__", "private", .y, "compute")

            reference <- reference_generator(
              start_date  = test_start_date,
              end_date    = test_end_date,
              slice_ts    = ds %.% slice_ts,
              source_conn = ds %.% source_conn,
              ds = ds
            )
          }
        )

        # Check that reference data is limited to the study period (start_date and end_date)
        reference_out_of_bounds <- reference |>
          dplyr::collect() |>
          dplyr::filter(.data$valid_until <= !!test_start_date | !!test_end_date < .data$valid_from)

        testthat::expect_identical(
          SCDB::nrow(reference_out_of_bounds),
          0L,
          info = glue::glue("Feature `{.x}` returns data outside the study period.")
        )

        # Check that valid_from / valid_until are date (or stored in the date-like class on the remote)
        validity_period_data_types <- reference |>
          utils::head(0) |>
          dplyr::select("valid_from", "valid_until") |>
          dplyr::collect() |>
          purrr::map(~ DBI::dbDataType(dbObj = conn, obj = .))

        testthat::expect_identical(
          purrr::pluck(validity_period_data_types, "valid_from"),
          DBI::dbDataType(dbObj = conn, obj = as.Date(0)),
          info = glue::glue("Feature `{.x}` has a non-Date `valid_from` column.")
        )
        testthat::expect_identical(
          purrr::pluck(validity_period_data_types, "valid_until"),
          DBI::dbDataType(dbObj = conn, obj = as.Date(0)),
          info = glue::glue("Feature `{.x}` has a non-Date `valid_until` column.")
        )

        # Check that valid_until (date or NA) is (strictly) greater than valid_from (date)
        # Remember that data is valid in the interval [valid_from, valid_until) and NA is treated as infinite
        testthat::expect_identical(sum(is.na(dplyr::pull(reference, "valid_from"))), 0L)

        testthat::expect_identical(
          sum(dplyr::pull(reference, "valid_from") >= dplyr::pull(reference, "valid_until"), na.rm = TRUE),
          0L,
          info = glue::glue("Feature `{.x}` has some elements where `valid_from` >= `valid_until`.")
        )


        # Copy to remote and continue checks
        if (!inherits(reference, "tbl_sql") ||
              (inherits(reference, "tbl_sql") && !identical(dbplyr::remote_con(reference), conn))) {
          reference <- dplyr::copy_to(conn, df = reference, name = SCDB::unique_table_name("ds_reference"))
          SCDB::defer_db_cleanup(reference)
        }

        reference_checksums <- reference |>
          SCDB::digest_to_checksum()

        # digest_to_checksum() creates an intermediary table in SQLite
        if (inherits(conn, "SQLiteConnection")) SCDB::defer_db_cleanup(reference_checksums)

        reference_checksums <- reference_checksums |>
          dplyr::pull("checksum") |>
          sort()


        testthat::expect_identical(feature_checksums, reference_checksums)

      })

      # Clean up
      connection_clean_up(conn)
      rm(ds)
      invisible(gc())
    }
  })

  # Set a test_end_date for the rest of the tests
  test_end_date <- test_start_date + lubridate::days(9)


  testthat::test_that(glue::glue("{diseasystore_class} can extend existing features"), {
    testthat::skip_if_not(local)

    for (conn in conn_generator(skip_backends)) {

      # Initialise without start_date and end_date
      ds <- testthat::expect_no_error(diseasystore_generator$new(verbose = FALSE, target_conn = conn, ...))

      # Attempt to get features from the feature store (using different dates)
      # then check that they match the expected value from the generators
      purrr::walk2(ds$available_features, ds$ds_map, ~ {

        # Suppress our user facing, informative warnings
        pkgcond::suppress_warnings(
          class = c(
            "diseasystore::age_on_date.SQLiteConnection-warning",
            "diseasystore::add_years.SQLiteConnection-warning"
          ),
          expr = {

            feature_checksums <- ds$get_feature(.x, start_date = test_start_date, end_date = test_end_date) |>
              SCDB::digest_to_checksum()

            # digest_to_checksum() creates an intermediary table in SQLite
            if (inherits(conn, "SQLiteConnection")) SCDB::defer_db_cleanup(feature_checksums)

            feature_checksums <- feature_checksums |>
              dplyr::pull("checksum") |>
              sort()


            reference_generator <- purrr::pluck(ds, ".__enclos_env__", "private", .y, "compute")

            reference <- reference_generator(
              start_date  = test_start_date,
              end_date    = test_end_date,
              slice_ts    = ds %.% slice_ts,
              source_conn = ds %.% source_conn,
              ds = ds
            )
          }
        )

        # Copy to remote and continue checks
        if (!inherits(reference, "tbl_sql") ||
              (inherits(reference, "tbl_sql") && !identical(dbplyr::remote_con(reference), conn))) {
          reference <- dplyr::copy_to(conn, df = reference, name = SCDB::unique_table_name("ds_reference"))
          SCDB::defer_db_cleanup(reference)
        }

        reference_checksums <- reference |>
          SCDB::digest_to_checksum()

        # digest_to_checksum() creates an intermediary table in SQLite
        if (inherits(conn, "SQLiteConnection")) SCDB::defer_db_cleanup(reference_checksums)

        reference_checksums <- reference_checksums |>
          dplyr::pull("checksum") |>
          sort()


        testthat::expect_identical(
          feature_checksums,
          reference_checksums,
          info = glue::glue("Feature `{.x}` has a mismatch between `$get_feature()` and `$compute()`.")
        )

      })

      # Clean up
      connection_clean_up(conn)
      rm(ds)
      invisible(gc())
    }
  })


  # Helper function that checks the output of key_joins
  key_join_features_tester <- function(output, start_date, end_date) {
    # The output dates should match start and end date
    testthat::expect_equal(min(output$date), start_date)                                                                # nolint: expect_identical_linter. R 4.5 makes dates more confusing than it already is
    testthat::expect_equal(max(output$date), end_date)                                                                  # nolint: expect_identical_linter.
  }


  testthat::test_that(glue::glue("{diseasystore_class} can key_join features"), {
    testthat::skip_if_not(local)

    for (conn in conn_generator(skip_backends)) {

      # Initialise without start_date and end_date
      ds <- testthat::expect_no_error(diseasystore_generator$new(verbose = FALSE, target_conn = conn, ...))

      # First check we can aggregate without a stratification
      for (observable in ds$available_observables) {
        testthat::expect_no_error(
          ds$key_join_features(observable = observable, stratification = NULL, test_start_date, test_end_date)
        )
      }



      # Then test combinations with non-NULL stratifications
      expand.grid(observable     = ds$available_observables,
                  stratification = ds$available_stratifications) |>
        purrr::pwalk(\(observable, stratification) {
          # This code may fail (gracefully) in some cases. These we catch here
          output <- tryCatch({
            ds$key_join_features(
              observable = as.character(observable),
              stratification = eval(parse(text = glue::glue("rlang::quos({stratification})"))),
              test_start_date, test_end_date
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
            key_join_features_tester(dplyr::collect(output), test_start_date, test_end_date)
          }

        })

      # Clean up
      connection_clean_up(conn)
      rm(ds)
      invisible(gc())
    }
  })


  testthat::test_that(glue::glue("{diseasystore_class} can key_join with feature-independent stratification"), {
    testthat::skip_if_not(local)

    for (conn in conn_generator(skip_backends)) {

      # Initialise without start_date and end_date
      ds <- testthat::expect_no_error(diseasystore_generator$new(verbose = FALSE, target_conn = conn, ...))

      if (length(ds$available_observables) > 0) {

        # Check we can aggregate with feature-independent stratifications
        output <- ds$key_join_features(
          observable = ds$available_observables[[1]],
          stratification = rlang::quos(string = "test", number = 2),
          test_start_date,
          test_end_date
        )

        testthat::expect_identical(unique(dplyr::pull(output, "string")), "test")
        testthat::expect_identical(unique(dplyr::pull(output, "number")), 2)
      }

      rm(ds)
      invisible(gc())
    }
  })


  testthat::test_that(glue::glue("{diseasystore_class} key_join fails gracefully"), {
    testthat::skip_if_not(local)

    for (conn in conn_generator(skip_backends)) {

      # Initialise without start_date and end_date
      ds <- testthat::expect_no_error(diseasystore_generator$new(verbose = FALSE, target_conn = conn, ...))

      # Attempt to perform the possible key_joins

      # Test key_join with malformed inputs
      expand.grid(observable     = ds$available_observables,
                  stratification = "non_existent_stratification") |>
        purrr::pwalk(\(observable, stratification) {
          # This code may fail (gracefully) in some cases. These we catch here
          output <- tryCatch({
            ds$key_join_features(
              observable = as.character(observable),
              stratification = eval(parse(text = glue::glue("rlang::quos({stratification})"))),
              start_date = test_start_date,
              end_date = test_end_date
            )
          }, error = function(e) {
            checkmate::expect_character(e$message, pattern = "Stratification could not be computed")
            return(NULL)
          })

          # If the code does not fail, we test the output
          if (!is.null(output)) {
            key_join_features_tester(dplyr::collect(output), test_start_date, test_end_date)
          }

        })


      expand.grid(observable     = ds$available_observables,
                  stratification = "test = non_existent_stratification") |>
        purrr::pwalk(\(observable, stratification) {
          # This code may fail (gracefully) in some cases. These we catch here
          output <- tryCatch({
            ds$key_join_features(
              observable = as.character(observable),
              stratification = eval(parse(text = glue::glue("rlang::quos({stratification})"))),
              start_date = test_start_date,
              end_date = test_end_date
            )
          }, error = function(e) {
            checkmate::expect_character(e$message, pattern = "Stratification could not be computed")
            return(NULL)
          })

          # If the code does not fail, we test the output
          if (!is.null(output)) {
            key_join_features_tester(dplyr::collect(output), test_start_date, test_end_date)
          }

        })

      # Clean up
      connection_clean_up(conn)
      rm(ds)
      invisible(gc())
    }
  })
}
