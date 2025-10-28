#' @title diseasystore base handler
#'
#' @description
#'   This `DiseasystoreBase` [R6][R6::R6Class] class forms the basis of all feature stores.
#'   It defines the primary methods of each feature stores as well as all of the public methods.
#' @examplesIf requireNamespace("RSQLite", quietly = TRUE)
#'   # DiseasystoreBase is mostly used as the basis of other, more specific, classes
#'   # The DiseasystoreBase can be initialised individually if needed.
#'
#'   ds <- DiseasystoreBase$new(source_conn = NULL,
#'                              target_conn = DBI::dbConnect(RSQLite::SQLite()))
#'
#'   rm(ds)
#' @return
#'   A new instance of the `DiseasystoreBase` [R6][R6::R6Class] class.
#' @export
#' @importFrom R6 R6Class
DiseasystoreBase <- R6::R6Class(                                                                                        # nolint: object_name_linter
  classname = "DiseasystoreBase",

  public = list(

    #' @description
    #'   Creates a new instance of the `DiseasystoreBase` [R6][R6::R6Class] class.
    #' @param start_date `r rd_start_date()`
    #' @param end_date `r rd_end_date()`
    #' @param slice_ts `r rd_slice_ts()`
    #' @param source_conn `r rd_source_conn()`
    #' @param target_conn `r rd_target_conn()`
    #' @param target_schema `r rd_target_schema()`
    #' @param verbose (`boolean`)\cr
    #'   Boolean that controls enables debugging information.
    #' @return
    #'   A new instance of the `DiseasystoreBase` [R6][R6::R6Class] class.
    initialize = function(start_date = NULL, end_date = NULL, slice_ts = NULL,
                          source_conn = NULL, target_conn = NULL, target_schema = NULL,
                          verbose = diseasyoption("verbose", self)) {

      # Validate input
      coll <- checkmate::makeAssertCollection()
      checkmate::assert_date(start_date, null.ok = TRUE, add = coll)
      checkmate::assert_date(end_date, null.ok = TRUE,   add = coll)
      checkmate::assert(
        checkmate::check_date(slice_ts),
        checkmate::check_posixct(slice_ts),
        checkmate::check_character(slice_ts, pattern = r"{^\d{4}-\d{2}-\d{2}(?: \d{2}:\d{2}:\d{2})?}", null.ok = TRUE),
        add = coll
      )
      checkmate::assert_logical(verbose, add = coll)
      checkmate::reportAssertions(coll)

      # Set internals
      if (!is.null(slice_ts))   private$.slice_ts   <- slice_ts
      if (!is.null(start_date)) private$.start_date <- start_date
      if (!is.null(end_date))   private$.end_date   <- end_date
      private$verbose <- verbose

      # Set the internal paths
      if (is.null(source_conn)) {
        source_conn <- diseasyoption("source_conn", self)
      }
      private$.source_conn <- parse_diseasyconn(source_conn, "source_conn")


      if (is.null(target_conn)) {
        target_conn <- diseasyoption("target_conn", self)
      }
      private$.target_conn <- parse_diseasyconn(target_conn, "target_conn")


      # Check source and target conn has been set correctly
      if (is.null(self %.% source_conn)) stop("source_conn option not defined for ", class(self)[1], call. = FALSE)
      if (is.null(self %.% target_conn)) stop("target_conn option not defined for ", class(self)[1], call. = FALSE)
      checkmate::assert_class(self %.% target_conn, "DBIConnection")


      if (is.null(target_schema)) {
        private$.target_schema <- diseasyoption("target_schema", self)
        if (is.null(self %.% target_schema)) {
          private$.target_schema <- "ds" # Default to "ds"
        }
      } else {
        private$.target_schema <- target_schema
      }
      checkmate::assert_character(self %.% target_schema)

    },


    #' @description
    #'   Computes, stores, and returns the requested feature for the study period.
    #' @param feature (`character`)\cr
    #'   The name of a feature defined in the feature store.
    #' @param start_date `r rd_start_date()`
    #' @param end_date `r rd_end_date()`
    #' @param slice_ts `r rd_slice_ts()`
    #' @return
    #'   A tbl_dbi with the requested feature for the study period.
    get_feature = function(feature,
                           start_date = self %.% start_date,
                           end_date   = self %.% end_date,
                           slice_ts   = self %.% slice_ts) {

      coll <- checkmate::makeAssertCollection()
      checkmate::assert_date(start_date, lower = self$min_start_date, add = coll)
      checkmate::assert_date(end_date,   upper = self$max_end_date, add = coll)
      checkmate::reportAssertions(coll)


      # Load the available features
      ds_map <- self %.% ds_map

      # Validate input
      coll <- checkmate::makeAssertCollection()
      checkmate::assert_choice(feature, names(ds_map), add = coll)
      checkmate::assert_date(start_date, any.missing = FALSE, add = coll)
      checkmate::assert_date(end_date,   any.missing = FALSE, add = coll)
      checkmate::assert(
        checkmate::check_date(slice_ts),
        checkmate::check_posixct(slice_ts),
        checkmate::check_character(slice_ts, pattern = r"{^\d{4}-\d{2}-\d{2}(?: \d{2}:\d{2}:\d{2})?}", null.ok = TRUE),
        add = coll
      )
      checkmate::assert(!is.null(self %.% source_conn), add = coll)
      checkmate::assert(!is.null(self %.% target_conn), add = coll)
      checkmate::reportAssertions(coll)

      # Determine which feature_loader should be called
      feature_loader <- purrr::pluck(ds_map, feature)

      # Determine where these features are stored
      target_table <- SCDB::id(paste(self %.% target_schema, feature_loader, sep = "."), self %.% target_conn)

      # Determine dates that need computation
      ds_missing_ranges <- self$determine_new_ranges(
        target_table = target_table,
        start_date   = start_date,
        end_date     = end_date,
        slice_ts     = slice_ts
      )

      # If there are any missing ranges, put a lock on the data base
      if (nrow(ds_missing_ranges) > 0) {

        # Add a LOCK to the diseasystore
        wait_time <- 0 # seconds
        while (!isTRUE(SCDB::lock_table(self %.% target_conn, target_table, self %.% target_schema))) {
          Sys.sleep(diseasyoption("lock_wait_increment", self))
          wait_time <- wait_time + diseasyoption("lock_wait_increment", self)
          if (wait_time > diseasyoption("lock_wait_max", self)) {
            stop(
              glue::glue("Lock not released within {diseasyoption('lock_wait_max', self)/60} minutes. Giving up."),
              call. = FALSE
            )
          }
        }

        # Once the locks are release
        # Re-determine dates that need computation
        ds_missing_ranges <- self$determine_new_ranges(
          target_table = target_table,
          start_date   = start_date,
          end_date     = end_date,
          slice_ts     = slice_ts
        )

        # Inform that we are computing features
        tic <- Sys.time()
        if (private %.% verbose && nrow(ds_missing_ranges) > 0) {
          message(glue::glue("feature: {feature} needs to be computed on the specified date interval. ",
                             "please wait..."))
        }

        # Call the feature loader on the dates
        purrr::pwalk(ds_missing_ranges, \(start_date, end_date) {

          # Compute the feature for the date range
          # Pass a reference to this diseasystore instance in case the `FeatureHandler` needs other features to compute
          ds_feature <- do.call(what = purrr::pluck(private, feature_loader) %.% compute,
                                args = list(start_date = start_date, end_date = end_date,
                                            slice_ts = slice_ts, source_conn = self %.% source_conn,
                                            ds = self))

          # Check it table is copied to target DB
          if (!inherits(ds_feature, "tbl_dbi") || !identical(self %.% source_conn, self %.% target_conn)) {
            ds_feature <- dplyr::copy_to(
              self %.% target_conn,
              df = ds_feature,
              name = SCDB::unique_table_name(paste0("ds_", feature_loader))
            )
            SCDB::defer_db_cleanup(ds_feature)
          }

          # Add the existing computed data for given slice_ts
          if (SCDB::table_exists(self %.% target_conn, target_table)) {
            ds_existing <- dplyr::tbl(self %.% target_conn, target_table)

            if (SCDB::is.historical(ds_existing)) {
              ds_existing <- ds_existing |>
                dplyr::filter(.data$from_ts == slice_ts) |>
                dplyr::select(!tidyselect::all_of(c("checksum", "from_ts", "until_ts"))) |>
                dplyr::filter(.data$valid_until <= start_date, .data$valid_from < end_date)
            }

            ds_updated_feature <- dplyr::union_all(ds_existing, ds_feature) |>
              dplyr::compute(name = SCDB::unique_table_name("ds_updated_feature"))
            SCDB::defer_db_cleanup(ds_updated_feature)
          } else {
            ds_updated_feature <- ds_feature
          }

          # Configure the Logger
          log_table_id <- SCDB::id(
            paste(self %.% target_schema, "logs", sep = "."),
            self %.% target_conn
          )

          logger <- SCDB::Logger$new(
            db_table = target_table,
            timestamp = slice_ts,
            output_to_console = FALSE,
            log_table_id = log_table_id,
            log_conn = self %.% target_conn
          )



          # Commit to DB
          SCDB::update_snapshot(
            .data = ds_updated_feature,
            conn = self %.% target_conn,
            db_table = target_table,
            timestamp = slice_ts,
            message = glue::glue("ds-range: {start_date} - {end_date}"),
            logger = logger,
            enforce_chronological_order = FALSE
          )
        })

        # Release the lock on the table
        SCDB::unlock_table(self %.% target_conn, target_table, self %.% target_schema)

        # Inform how long has elapsed for updating data
        if (private$verbose && nrow(ds_missing_ranges) > 0) {
          message(glue::glue("feature: {feature} updated ",
                             "(elapsed time {format(round(difftime(Sys.time(), tic)),2)})."))
        }
      }

      # Finally, return the data to the user
      out <- do.call(what = purrr::pluck(private, feature_loader) %.% get,
                     args = list(target_table = target_table,
                                 slice_ts = slice_ts, target_conn = self %.% target_conn))

      # We need to slice to the period of interest.
      # to ensure proper conversion of variables, we first copy the limits over and then do an inner_join
      validities <- dplyr::copy_to(
        self %.% target_conn,
        df = data.frame(valid_from = start_date, valid_until = end_date),
        name = SCDB::unique_table_name("ds_validities")
      )
      SCDB::defer_db_cleanup(validities)

      out <- dplyr::inner_join(out, validities,
                               sql_on = '"LHS"."valid_from" <= "RHS"."valid_until" AND
                                         ("LHS"."valid_until" > "RHS"."valid_from" OR "LHS"."valid_until" IS NULL)',
                               suffix = c("", ".p")) |>
        dplyr::select(!c("valid_from.p", "valid_until.p")) |>
        dplyr::compute(name = SCDB::unique_table_name("ds_get_feature"))

      return(out)
    },

    #' @description
    #'   Joins various features from the feature store assuming a primary feature (observable)
    #'   that contains keys to witch the secondary features (defined by `stratification`) are joined.
    #' @param observable `r rd_observable()`
    #' @param stratification `r rd_stratification()`
    #' @param start_date `r rd_start_date()`
    #' @param end_date `r rd_end_date()`
    #' @return
    #'   A tbl_dbi with the requested joined features for the study period.
    #' @importFrom dbplyr window_order
    key_join_features = function(observable, stratification = NULL,
                                 start_date = self %.% start_date,
                                 end_date   = self %.% end_date) {

      coll <- checkmate::makeAssertCollection()
      checkmate::assert_choice(observable, self$available_observables, add = coll)
      checkmate::assert_multi_class(stratification, c("quosure", "quosures"), null.ok = TRUE, add = coll)
      checkmate::assert_date(start_date, add = coll)
      checkmate::assert_date(end_date, add = coll)
      checkmate::reportAssertions(coll)

      # Store the ds_map
      ds_map <- self %.% ds_map

      # We start by copying the study_dates to the conn to ensure SQLite compatibility
      study_dates <- dplyr::copy_to(
        self %.% target_conn,
        df = data.frame(valid_from = start_date, valid_until = base::as.Date(end_date + lubridate::days(1))),
        name = SCDB::unique_table_name("ds_study_dates")
      )
      SCDB::defer_db_cleanup(study_dates)

      # Fetch the requested observable from the feature store and truncate to the start and end dates
      # to simplify the interlaced output
      observable_data <- self$get_feature(observable, start_date, end_date)
      SCDB::defer_db_cleanup(observable_data)

      observable_data <- observable_data |>
        dplyr::cross_join(study_dates, suffix = c("", ".d")) |>
        dplyr::mutate(
          "valid_from" = ifelse(.data$valid_from >= .data$valid_from.d, .data$valid_from, .data$valid_from.d),          # nolint: ifelse_censor_linter
          "valid_until" = dplyr::coalesce(
            ifelse(.data$valid_until <= .data$valid_until.d, .data$valid_until, .data$valid_until.d),                   # nolint: ifelse_censor_linter
            .data$valid_until.d
          )
        ) |>
        dplyr::select(!ends_with(".d"))

      # Determine the keys
      observable_keys <- colnames(dplyr::select(observable_data, tidyselect::starts_with("key_")))

      # Give warning if stratification features are already in the observables data
      # First we identify the computations being done in the stratifications, only stratifications that compute a new
      # entity can lead to unexpected behaviour. If the user just request a already existing stratification, there will
      # be no ambiguities.
      new_stratifications <- stratification |>
        purrr::map(rlang::as_label) |>
        names() |>
        purrr::discard(~ . == "")

      # .. and then we look for overlap with existing stratifications
      existing_stratification <- intersect(colnames(observable_data), new_stratifications)
      if (length(existing_stratification) > 0) {
        warning(
          "Observable already stratified by: ",
          toString(existing_stratification), ". ",
          "Output might be inconsistent with expectation.",
          call. = FALSE
        )
      }


      # Determine which features are affected by a stratification
      if (is.null(stratification)) {

        stratification_features <- NULL
        stratification_names <- NULL
        stratification_data <- NULL
        out <- observable_data

      } else {

        # Create regex detection for features
        ds_map_regex <- paste0(r"{(?<=^|\W)}", names(ds_map), r"{(?=$|\W)}")

        # Perform detection of features in the stratification
        stratification_features <- stratification |>
          purrr::map(~ rlang::quo_text(., width = 500L)) |> # Convert expr to string we can parse
          purrr::map(\(label) {
            purrr::map(ds_map_regex, ~ stringr::str_extract(label, .x)) |>
              purrr::discard(is.na)
          }) |>
          unlist() |>
          unique()

        # Determine the name of the columns created by the stratifications
        stratification_names <- stratification |>
          purrr::map(rlang::as_label) |>
          purrr::imap_chr(~ ifelse(.y == "", .x, .y)) |>
          unname()

        # Check stratification names do not collide with any of the observables
        stopifnot("Stratification features cannot be observables" =
                    purrr::none(stratification_names, ~ . %in% self$available_observables))

        # Fetch requested stratification features from the feature store
        stratification_data <- stratification_features |>
          purrr::discard(~ . %in% colnames(observable_data)) |> # Skip those already in the observable data
          unique() |>
          purrr::map(~ self$get_feature(.x, start_date, end_date))

        # Check if no stratification data was pulled
        if (length(stratification_data) == 0) {

          stratification_data <- NULL
          out <- observable_data

        } else {

          # Pre-truncate stratification data to the start and end dates to simplify the interlaced output
          stratification_data_truncated <- stratification_data |>
            purrr::map(\(feature) {
              feature |>
                dplyr::cross_join(study_dates, suffix = c("", ".d")) |>
                dplyr::mutate(
                  "valid_from" = ifelse(.data$valid_from >= .data$valid_from.d, .data$valid_from, .data$valid_from.d),  # nolint: ifelse_censor_linter
                  "valid_until" = dplyr::coalesce(
                    ifelse(.data$valid_until <= .data$valid_until.d, .data$valid_until, .data$valid_until.d),           # nolint: ifelse_censor_linter
                    .data$valid_until.d
                  )
                ) |>
                dplyr::select(!ends_with(".d"))
            })

          # Merge stratifications to the observables
          out <- truncate_interlace(observable_data, stratification_data_truncated)
          if (length(stratification_data_truncated) == 1) {
            out <- dplyr::compute(out, name = SCDB::unique_table_name("ds_truncate_interlace"))
          }
          SCDB::defer_db_cleanup(out)

          # Stratification data is no longer needed
          purrr::walk(stratification_data, SCDB::defer_db_cleanup)
        }
      }


      # Run the filtering needed for semi-aggregated data
      out <- private$key_join_filter(out, stratification_features, start_date, end_date)

      # Retrieve the aggregators (and ensure they work together)
      key_join_aggregators <- c(purrr::pluck(private, purrr::pluck(ds_map, observable)) %.% key_join,
                                purrr::map(stratification_features,
                                           ~ purrr::pluck(private, purrr::pluck(ds_map, .x)) %.% key_join))

      if (length(unique(key_join_aggregators)) > 1) {
        stop(
          "(At least one) stratification feature does not match observable aggregator. Not implemented yet.",
          call. = FALSE
        )
      }

      key_join_aggregator <- purrr::pluck(key_join_aggregators, 1)

      # Check stratification can be performed:
      out <- tryCatch(
        dplyr::group_by(out, !!!stratification),
        error = function(e) {
          error_msg <- glue::glue(
            "Stratification could not be computed. ",
            "Error {tolower(substr(e$message, 1, 1))}{substr(e$message, 2, nchar(e$message))}. ",
            "Available stratification variables are: ",
            "{toString(self$available_stratifications)}"
          )
          stop(error_msg, call. = FALSE)
        }
      )

      # Add the new valid counts
      t_add <- out |>
        dplyr::group_by("date" = .data$valid_from, .add = TRUE) |>
        key_join_aggregator(observable) |>
        dplyr::rename("n_add" = "n") |>
        dplyr::compute(name = SCDB::unique_table_name("ds_add"))
      SCDB::defer_db_cleanup(t_add)

      # Add the new invalid counts
      t_remove <- out |>
        dplyr::group_by("date" = .data$valid_until, .add = TRUE) |>
        key_join_aggregator(observable) |>
        dplyr::rename("n_remove" = "n") |>
        dplyr::compute(name = SCDB::unique_table_name("ds_remove"))
      SCDB::defer_db_cleanup(t_remove)

      # Get all combinations to merge onto
      all_dates <- dplyr::copy_to(
        self %.% target_conn,
        df = tibble::tibble(date = as.Date(as.numeric(seq.Date(from = start_date, to = end_date, by = 1)))), # PR #220
        name = SCDB::unique_table_name("ds_all_dates")
      )
      SCDB::defer_db_cleanup(all_dates)

      if (is.null(stratification)) {
        all_combinations <- all_dates
      } else {
        all_combinations <- out |>
          dplyr::select(dplyr::all_of(stratification_names)) |>
          dplyr::distinct() |>
          dplyr::cross_join(all_dates) |>
          dplyr::compute(name = SCDB::unique_table_name("ds_all_combinations"))
        SCDB::defer_db_cleanup(all_combinations)
      }

      # Aggregate across dates
      data <- t_add |>
        dplyr::right_join(all_combinations, by = c("date", stratification_names), na_matches = "na") |>
        dplyr::left_join(t_remove,  by = c("date", stratification_names), na_matches = "na") |>
        tidyr::replace_na(list("n_add" = 0, "n_remove" = 0)) |>
        dplyr::group_by(dplyr::across(tidyselect::all_of(stratification_names))) |>
        dbplyr::window_order(date) |>
        dplyr::mutate(!!observable := cumsum(.data$n_add) - cumsum(.data$n_remove)) |>
        dplyr::ungroup() |>
        dplyr::select("date", all_of(stratification_names), !!observable) |>
        dplyr::collect()

      # Ensure date is of type Date
      data <- data |>
        dplyr::mutate("date" = as.Date(.data$date))

      return(data)
    },


    #' @description
    #'   Determine which dates are not already computed.
    #'
    #'   This method parses the `message` fields of the logs related to `target_table` on the date given by `slice_ts`.
    #'   These messages contains information on the date-ranges that are already computed.
    #'   Once parsed, the method compares the computed date-ranges with the requested date-range.
    #' @param target_table (`character` or `Id`)\cr
    #'   The feature table to investigate
    #' @param start_date `r rd_start_date()`
    #' @param end_date `r rd_end_date()`
    #' @param slice_ts `r rd_slice_ts()`
    #' @return (`tibble`)\cr
    #'   A data frame containing continuous un-computed date-ranges
    determine_new_ranges = function(target_table, start_date, end_date, slice_ts) {

      # Create log table
      SCDB::create_logs_if_missing(
        log_table = paste(self %.% target_schema, "logs", sep = "."),
        conn = self %.% target_conn
      )

      # Get a list of the logs for the target_table on the slice_ts
      logs <- dplyr::tbl(
        self %.% target_conn,
        SCDB::id(paste(self %.% target_schema, "logs", sep = "."), self %.% target_conn)
      ) |>
        dplyr::filter(.data$date == !!SCDB::db_timestamp(slice_ts, self %.% target_conn)) |>
        dplyr::collect() |>
        tidyr::unite("target_table", tidyselect::any_of(c("catalog", "schema", "table")), sep = ".", na.rm = TRUE) |>
        dplyr::filter(
          .data$target_table == !!as.character(target_table)
        )

      # If no logs are found, we need to compute on the entire range
      if (nrow(logs) == 0) {
        return(tibble::tibble(start_date = start_date, end_date = end_date))
      }

      # Determine the date ranges used
      logs <- logs |>
        dplyr::mutate("ds_start_date" = stringr::str_extract(.data$message, r"{(?<=ds-range: )(\d{4}-\d{2}-\d{2})}"),
                      "ds_end_date"   = stringr::str_extract(.data$message, r"{(\d{4}-\d{2}-\d{2})$}")) |>
        dplyr::mutate(across(.cols = c("ds_start_date", "ds_end_date"), .fns = base::as.Date))

      # Find updates that overlap with requested range
      logs <- logs |>
        dplyr::filter(
          .data$ds_start_date <= {{ end_date }},
          {{ start_date }} <= .data$ds_end_date,
          .data$success == TRUE                                                                                         # nolint: redundant_equals_linter. Required for dbplyr translations
        )

      # Determine the dates covered on this slice_ts
      if (nrow(logs) > 0) {
        ds_dates <- logs |>
          dplyr::transmute("ds_start_date" = base::as.Date(ds_start_date),
                           "ds_end_date" = base::as.Date(ds_end_date)) |>
          purrr::pmap(\(ds_start_date, ds_end_date) seq.Date(from = ds_start_date, to = ds_end_date, by = "1 day")) |>
          purrr::reduce(dplyr::union_all) |> # union does not preserve type (converts from Date to numeric)
          unique() # so we have to use union_all (preserves type) followed by unique (preserves type)
      } else {
        ds_dates <- list()
      }

      # Define the new dates to compute
      new_interval <- seq.Date(from = base::as.Date(start_date), to = base::as.Date(end_date), by = "1 day")

      # Determine the dates that needs to be computed
      new_dates <- as.Date(setdiff(new_interval, ds_dates))
      # setdiff does not preserve type (converts from Date to numeric)

      # Early return, if no new dates are found
      if (length(new_dates) == 0) {
        return(tibble::tibble(start_date = base::as.Date(character(0)), end_date = base::as.Date(character(0))))
      }

      # Reduce to single intervals
      new_ranges <- tibble::tibble(date = new_dates) |>
        dplyr::mutate("prev_date_diff" = as.numeric(difftime(.data$date, dplyr::lag(.data$date), units = "days")),
                      "first_in_segment" = dplyr::case_when(
                        is.na(.data$prev_date_diff) ~ TRUE,   # Nothing before, must be a new segment
                        .data$prev_date_diff > 1 ~ TRUE,      # Previous segment is long before, must be a new segment
                        TRUE ~ FALSE                          # All other cases are not the first in segment
                      )) |>
        dplyr::group_by(cumsum(.data$first_in_segment)) |>
        dplyr::summarise(start_date = min(.data$date, na.rm = TRUE),
                         end_date   = max(.data$date, na.rm = TRUE),
                         .groups = "drop") |>
        dplyr::select("start_date", "end_date")

      return(new_ranges)
    }
  ),

  active = list(

    #' @field ds_map (`named list`(`character`))\cr
    #'   A list that maps features known by the feature store to the corresponding feature handlers
    #'   that compute the features. Read only.
    ds_map = purrr::partial(
      .f = active_binding,
      name = "ds_map",
      expr = {

        # If the class is "DiseasystoreBase", we break the iteration, otherwise we recursively iterate deeper
        if (exists("super")) {
          return(c(super$.ds_map, private %.% .ds_map))
        } else {
          return(private %.% .ds_map)
        }
      }
    ),


    #' @field available_features (`character()`)\cr
    #'   A list of available features in the feature store. Read only.
    available_features = purrr::partial(
      .f = active_binding,
      name = "available_features",
      expr = return(names(self$ds_map))
    ),


    #' @field available_observables (`character()`)\cr
    #'   A list of available observables in the feature store. Read only.
    available_observables = purrr::partial(
      .f = active_binding,
      name = "available_observables",
      expr = return(purrr::keep(self$available_features, ~ stringr::str_detect(., self$observables_regex)))
    ),


    #' @field available_stratifications (`character()`)\cr
    #'   A list of available stratifications in the feature store. Read only.
    available_stratifications = purrr::partial(
      .f = active_binding,
      name = "available_stratifications",
      expr = return(purrr::discard(self$available_features, ~ stringr::str_detect(., self$observables_regex)))
    ),


    #' @field observables_regex (`character(1)`)\cr
    #'   A list of available stratifications in the feature store. Read only.
    observables_regex = purrr::partial(
      .f = active_binding,
      name = "observables_regex",
      expr = return(private$.observables_regex)
    ),


    #' @field label (`character(1)`)\cr
    #'   A human readable label of the feature store. Read only.
    label = purrr::partial(
      .f = active_binding,
      name = "label",
      expr = return(private$.label)
    ),


    #' @field source_conn `r rd_source_conn("field")`
    source_conn = purrr::partial(
      .f = active_binding,
      name = "source_conn",
      expr = {
        if (!is.null(private$.source_conn)) {
          return(private$.source_conn)
        } else {
          return(private$.target_conn)
        }
      }
    ),


    #' @field target_conn `r rd_target_conn("field")`
    target_conn = purrr::partial(
      .f = active_binding,
      name = "target_conn",
      expr = return(private$.target_conn)
    ),


    #' @field target_schema `r rd_target_schema("field")`
    target_schema = purrr::partial(
      .f = active_binding,
      name = "target_schema",
      expr = return(private$.target_schema)
    ),


    #' @field start_date `r rd_start_date("field")`
    start_date = purrr::partial(
      .f = active_binding,
      name = "start_date",
      expr = return(private$.start_date)
    ),


    #' @field end_date `r rd_end_date("field")`
    end_date = purrr::partial(
      .f = active_binding,
      name = "end_date",
      expr = return(private$.end_date)
    ),


    #' @field min_start_date `r rd_start_date("field", minimum = TRUE)`
    min_start_date = purrr::partial(
      .f = active_binding,
      name = "min_start_date",
      expr = return(private$.min_start_date)
    ),


    #' @field max_end_date `r rd_end_date("field", maximum = TRUE)`
    max_end_date = purrr::partial(
      .f = active_binding,
      name = "max_end_date",
      expr = return(private$.max_end_date)
    ),


    #' @field slice_ts `r rd_slice_ts("field")`
    slice_ts = purrr::partial(
      .f = active_binding,
      name = "slice_ts",
      expr = return(private$.slice_ts)
    )
  ),

  private = list(

    .label         = NULL,
    .source_conn   = NULL,
    .target_conn   = NULL,
    .target_schema = NULL,

    .start_date = NULL,
    .end_date   = NULL,
    .min_start_date = NULL,
    .max_end_date   = NULL,
    .slice_ts   = lubridate::today(),

    .ds_map     = NULL, # Must be implemented in child classes
    ds_key_map  = NULL, # Must be implemented in child classes

    .observables_regex = r"{^n_(?=\w)}",

    verbose = TRUE,


    # @description
    #   This function implements an intermediate filtering in the stratification pipeline.
    #   For semi-aggregated data like Google's COVID-19 data, some people are counted more than once.
    #   The `key_join_filter` is inserted into the stratification pipeline to remove this double counting.
    # @param .data `r rd_.data()`
    # @param stratification_features (`character`)\cr
    #   A list of the features included in the stratification process.
    # @param start_date `r rd_start_date()`
    # @param end_date `r rd_end_date()`
    # @return
    #   A subset of `.data` filtered to remove double counting
    key_join_filter = function(.data, stratification_features,
                               start_date = self %.% start_date,
                               end_date   = self %.% end_date) {
      return(.data) # By default, no filtering is performed
    },


    # @description
    #   Closes the open DB connection when removing the object
    finalize = function() {
      list(self %.% target_conn, self %.% source_conn) |>
        purrr::keep(~ inherits(., "DBIConnection")) |>
        purrr::discard(~ inherits(., "TestConnection")) |>
        purrr::keep(~ DBI::dbIsValid(.)) |>
        purrr::walk(DBI::dbDisconnect)
    }
  )
)

# Set default options for the package related to the Google COVID-19 store
rlang::on_load({
  options("diseasystore.source_conn" = "")
  options("diseasystore.target_conn" = "")
  options("diseasystore.target_schema" = "ds")
  options("diseasystore.verbose" = TRUE)
  options("diseasystore.lock_wait_max" = 30 * 60) # 30 minutes
  options("diseasystore.lock_wait_increment" = 15) # 15 seconds
})
