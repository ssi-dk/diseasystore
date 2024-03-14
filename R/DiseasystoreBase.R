#' @title diseasystore base handler
#'
#' @description
#'   This `DiseasystoreBase` [R6][R6::R6Class] class forms the basis of all feature stores.
#'   It defines the primary methods of each feature stores as well as all of the public methods.
#' @examples
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
      if (is.null(self %.% source_conn)) stop("source_conn option not defined for ", class(self)[1])
      if (is.null(self %.% target_conn)) stop("target_conn option not defined for ", class(self)[1])
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
    #'   Closes the open DB connection when removing the object
    finalize = function() {
      purrr::walk(list(self %.% target_conn, self %.% source_conn),
                  ~ if (inherits(., "DBIConnection") && !inherits(., "TestConnection") && DBI::dbIsValid(.)) {
                    DBI::dbDisconnect(.)
                  })
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

      # Load the available features
      ds_map <- self %.% ds_map

      # Validate input
      coll <- checkmate::makeAssertCollection()
      checkmate::assert_choice(feature, names(ds_map), add = coll)
      checkmate::assert_date(start_date, any.missing = FALSE, add = coll)
      checkmate::assert_date(end_date,   any.missing = FALSE, add = coll)
      checkmate::assert(
        checkmate::check_date(slice_ts),
        checkmate::check_character(slice_ts, pattern = r"{^\d{4}-\d{2}-\d{2}(?: \d{2}:\d{2}:\d{2})?}", null.ok = TRUE),
        add = coll
      )
      checkmate::assert(!is.null(self %.% source_conn), add = coll)
      checkmate::assert(!is.null(self %.% target_conn), add = coll)
      checkmate::reportAssertions(coll)

      # Determine which feature_loader should be called
      feature_loader <- purrr::pluck(ds_map, feature)

      # Determine where these features are stored
      target_table_id <- SCDB::id(paste(self %.% target_schema, feature_loader, sep = "."), self %.% target_conn)

      if (packageVersion("SCDB") <= "0.3") {
        target_table <- paste(
          c(purrr::pluck(target_table_id, "name", "schema"),
            purrr::pluck(target_table_id, "name", "table")
          ),
          collapse = "."
        )
      } else {
        target_table <- target_table_id # From SCDB 0.4 we can use the Id everywhere
      }


      # Create log table
      SCDB::create_logs_if_missing(log_table = paste(self %.% target_schema, "logs", sep = "."),
                                   conn = self %.% target_conn)

      # Determine dates that need computation
      ds_missing_ranges <- private$determine_new_ranges(target_table = target_table,
                                                        start_date   = start_date,
                                                        end_date     = end_date,
                                                        slice_ts     = slice_ts)

      # If there are any missing ranges, put a lock on the data base
      if (nrow(ds_missing_ranges) > 0) {

        # Add a LOCK to the diseasystore
        add_table_lock(self %.% target_conn, target_table, self %.% target_schema)

        # Check if we have ownership of the update of the target_table
        # If not, keep retrying and wait for up to 30 minutes before giving up
        wait_time <- 0 # seconds
        while (!isTRUE(is_lock_owner(self %.% target_conn, target_table, self %.% target_schema))) {
          Sys.sleep(diseasyoption("lock_wait_increment"))
          add_table_lock(self %.% target_conn, target_table, self %.% target_schema)
          wait_time <- wait_time + diseasyoption("lock_wait_increment")
          if (wait_time > diseasyoption("lock_wait_max")) {
            rlang::abort(glue::glue("Lock not released within {diseasyoption('lock_wait_max')/60} minutes. Giving up."))
          }
        }

        # Once the locks are release
        # Re-determine dates that need computation
        ds_missing_ranges <- private$determine_new_ranges(target_table = target_table,
                                                          start_date   = start_date,
                                                          end_date     = end_date,
                                                          slice_ts     = slice_ts)

        # Inform that we are computing features
        tic <- Sys.time()
        if (private %.% verbose && nrow(ds_missing_ranges) > 0) {
          message(glue::glue("feature: {feature} needs to be computed on the specified date interval. ",
                             "please wait..."))
        }

        # Call the feature loader on the dates
        purrr::pwalk(ds_missing_ranges, \(start_date, end_date) {

          # Compute the feature for the date range
          ds_feature <- do.call(what = purrr::pluck(private, feature_loader) %.% compute,
                                args = list(start_date = start_date, end_date = end_date,
                                            slice_ts = slice_ts, source_conn = self %.% source_conn))

          # Check it table is copied to target DB
          if (!inherits(ds_feature, "tbl_dbi") || !identical(self %.% source_conn, self %.% target_conn)) {
            ds_feature <- ds_feature |>
              dplyr::copy_to(self %.% target_conn, df = _,
                             name = paste("ds", feature_loader, Sys.getpid(), sep = "_"), overwrite = TRUE)
          }

          # Add the existing computed data for given slice_ts
          if (SCDB::table_exists(self %.% target_conn, target_table)) {
            ds_existing <- dplyr::tbl(self %.% target_conn, target_table_id, check_from = FALSE)

            if (SCDB::is.historical(ds_existing)) {
              ds_existing <- ds_existing |>
                dplyr::filter(.data$from_ts == slice_ts) |>
                dplyr::select(!tidyselect::all_of(c("checksum", "from_ts", "until_ts"))) |>
                dplyr::filter(.data$valid_until <= start_date, .data$valid_from < end_date)
            }

            ds_updated_feature <- dplyr::union_all(ds_existing, ds_feature) |> dplyr::compute()
          } else {
            ds_updated_feature <- ds_feature
          }

          # Configure the Logger
          log_table_id <- SCDB::id(
            paste(self %.% target_schema, "logs", sep = "."),
            self %.% target_conn
          )

          logger <- SCDB::Logger$new(
            output_to_console = FALSE,
            log_table_id = log_table_id,
            log_conn = self %.% target_conn
          )

          # Commit to DB
          SCDB::update_snapshot(
            .data = ds_updated_feature,
            conn = self %.% target_conn,
            db_table = target_table_id,
            timestamp = slice_ts,
            message = glue::glue("ds-range: {start_date} - {end_date}"),
            logger = logger,
            enforce_chronological_order = FALSE
          )
        })

        # Release the lock on the table
        remove_table_lock(self %.% target_conn, target_table, self %.% target_schema)

        # Inform how long has elapsed for updating data
        if (private$verbose && nrow(ds_missing_ranges) > 0) {
          message(glue::glue("feature: {feature} updated ",
                             "(elapsed time {format(round(difftime(Sys.time(), tic)),2)})."))
        }
      }

      # Finally, return the data to the user
      out <- do.call(what = purrr::pluck(private, feature_loader) %.% get,
                     args = list(target_table = target_table_id,
                                 slice_ts = slice_ts, target_conn = self %.% target_conn))

      # We need to slice to the period of interest.
      # to ensure proper conversion of variables, we first copy the limits over and then do an inner_join
      validities <- data.frame(valid_from = start_date, valid_until = end_date) |>
        dplyr::copy_to(self %.% target_conn, df = _, name = paste0("ds_validities_", Sys.getpid()), overwrite = TRUE)

      out <- dplyr::inner_join(out, validities,
                               sql_on = '"LHS"."valid_from" <= "RHS"."valid_until" AND
                                         ("LHS"."valid_until" > "RHS"."valid_from" OR "LHS"."valid_until" IS NULL)',
                               suffix = c("", ".p")) |>
        dplyr::select(!c("valid_from.p", "valid_until.p")) |>
        dplyr::compute()

      return(out)
    },

    #' @description
    #'   Joins various features from feature store assuming a primary feature (observable)
    #'   that contains keys to witch the secondary features (defined by `stratification`) can be joined.
    #' @param observable (`character`)\cr
    #'   The name of a feature defined in the feature store
    #' @param stratification (`list`(`quosures`))\cr
    #'   Expressions in `stratification` are evaluated to find appropriate features.
    #'   These are then joined to the observable feature before `stratification` is performed.
    #' @param start_date `r rd_start_date()`
    #' @param end_date `r rd_end_date()`
    #' @return
    #'   A tbl_dbi with the requested joined features for the study period.
    #' @importFrom dbplyr window_order
    key_join_features = function(observable, stratification,
                                 start_date = self %.% start_date,
                                 end_date   = self %.% end_date) {

      # Validate input
      available_observables  <- self$available_features |>
        purrr::keep(~ startsWith(., "n_") | endsWith(., "_temperature"))
      available_stratifications <- self$available_features |>
        purrr::discard(~ startsWith(., "n_") | endsWith(., "_temperature"))

      coll <- checkmate::makeAssertCollection()
      checkmate::assert_choice(observable, available_observables, add = coll)
      checkmate::assert(
        checkmate::check_list(stratification, types = "character", null.ok = TRUE),
        checkmate::check_multi_class(stratification, c("character", "quosure", "quosures"), null.ok = TRUE),
        add = coll
      )
      checkmate::assert_date(start_date, add = coll)
      checkmate::assert_date(end_date, add = coll)
      checkmate::reportAssertions(coll)

      # Store the ds_map
      ds_map <- self %.% ds_map

      # We start by copying the study_dates to the conn to ensure SQLite compatibility
      study_dates <- data.frame(valid_from = start_date, valid_until = base::as.Date(end_date + lubridate::days(1))) |>
        dplyr::copy_to(self %.% target_conn, df = _, name = paste0("ds_study_dates_", Sys.getpid()), overwrite = TRUE)

      # Determine which features are affected by a stratification
      if (!is.null(stratification)) {

        # Create regex detection for features
        ds_map_regex <- paste0(r"{(?<=^|\W)}", names(ds_map), r"{(?=$|\W)}")

        # Perform detection of features in the stratification
        stratification_features <- purrr::map(stratification, rlang::as_label) |>
          purrr::map(\(label) {
            purrr::map(ds_map_regex, ~ stringr::str_extract(label, .x)) |>
              purrr::discard(is.na)
          }) |>
          unlist() |>
          unique()

        # Report if stratification not found
        if (is.null(stratification_features)) {
          err <- glue::glue("Stratification variable not found. ",
                            "Available stratification variables are: ",
                            "{toString(available_stratifications)}")
          stop(err)
        }

        stratification_names <- purrr::map(stratification, rlang::as_label)
        stratification_names <- purrr::map2_chr(stratification_names,
                                                names(stratification_names),
                                                ~ ifelse(.y == "", .x, .y)) |>
          unname()

        # Check stratification features are not observables
        stopifnot("Stratification features cannot be observables" =
                    purrr::none(stratification_names, ~ . %in% available_observables))

        # Fetch requested stratification features from the feature store
        stratification_data <- stratification_features |>
          unique() |>
          purrr::map(~ {
            # Fetch the requested stratification feature from the feature store and truncate to the start
            #  and end dates to simplify the interlaced output
            self$get_feature(.x, start_date, end_date) |>
              dplyr::cross_join(study_dates, suffix = c("", ".d")) |>
              dplyr::mutate(
                "valid_from" = ifelse(.data$valid_from >= .data$valid_from.d, .data$valid_from, .data$valid_from.d),    # nolint: ifelse_censor_linter
                "valid_until" = dplyr::coalesce(
                  ifelse(.data$valid_until <= .data$valid_until.d, .data$valid_until, .data$valid_until.d),             # nolint: ifelse_censor_linter
                  .data$valid_until.d
                )
              ) |>
              dplyr::select(!ends_with(".d"))
          })
      } else {
        stratification_features <- NULL
        stratification_names <- NULL
        stratification_data <- NULL
      }

      # Fetch the requested observable from the feature store and truncate to the start and end dates
      # to simplify the interlaced output
      observable_data <- self$get_feature(observable, start_date, end_date) |>
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
      observable_keys  <- colnames(dplyr::select(observable_data, tidyselect::starts_with("key_")))

      # Give warning if stratification features are already in the observables data
      existing_stratification <- intersect(colnames(observable_data), stratification_features)
      if (length(existing_stratification) > 0) {
        warning("Observable already stratified by: ", toString(existing_stratification), ". ",
                "Output might be inconsistent with expectation.")
      }

      # Map stratification_data to observable_keys (if not already)
      if (!is.null(stratification_data)) {
        stratification_keys <- purrr::map(stratification_data,
                                          ~ colnames(dplyr::select(., tidyselect::starts_with("key_"))))

        stratification_data <- stratification_data |>
          purrr::map_if(!purrr::map_lgl(stratification_keys, ~ any(observable_keys %in% .)),
                        ~ .)
      }

      # Merge and prepare for counting
      out <- truncate_interlace(observable_data, stratification_data) |>
        private$key_join_filter(stratification_features, start_date, end_date) |>
        dplyr::compute()

      # Retrieve the aggregators (and ensure they work together)
      key_join_aggregators <- c(purrr::pluck(private, purrr::pluck(ds_map, observable)) %.% key_join,
                                purrr::map(stratification_features,
                                           ~ purrr::pluck(private, purrr::pluck(ds_map, .x)) %.% key_join))

      if (length(unique(key_join_aggregators)) > 1) {
        stop("(At least one) stratification feature does not match observable aggregator. Not implemented yet.")
      }

      key_join_aggregator <- purrr::pluck(key_join_aggregators, 1)

      # Add the new valid counts
      t_add <- out |>
        dplyr::group_by(!!!stratification) |>
        dplyr::group_by(date = valid_from, .add = TRUE) |>
        key_join_aggregator(observable) |>
        dplyr::rename(n_add = n) |>
        dplyr::compute()

      # Add the new invalid counts
      t_remove <- out |>
        dplyr::group_by(!!!stratification) |>
        dplyr::group_by(date = valid_until, .add = TRUE) |>
        key_join_aggregator(observable) |>
        dplyr::rename(n_remove = n) |>
        dplyr::compute()

      # Get all combinations to merge onto
      all_dates <- tibble::tibble(date = seq.Date(from = start_date, to = end_date, by = 1))

      if (!is.null(stratification)) {
        all_combinations <- out |>
          dplyr::ungroup() |>
          dplyr::distinct(!!!stratification) |>
          dplyr::cross_join(all_dates, copy = TRUE) |>
          dplyr::compute()
      } else {
        all_combinations <- all_dates

        # Copy if needed
        if (is.null(stratification)) {
          all_combinations <- all_combinations |>
            dplyr::copy_to(self %.% target_conn, df = _,
                           name = paste0("ds_all_combinations_", Sys.getpid()), overwrite = TRUE)
        }
      }

      # Aggregate across dates
      data <- t_add |>
        dplyr::right_join(all_combinations, by = c("date", stratification_names), na_matches = "na") |>
        dplyr::left_join(t_remove,  by = c("date", stratification_names), na_matches = "na") |>
        tidyr::replace_na(list(n_add = 0, n_remove = 0)) |>
        dplyr::group_by(dplyr::across(tidyselect::all_of(stratification_names))) |>
        dbplyr::window_order(date) |>
        dplyr::mutate(date, !!observable := cumsum(n_add) - cumsum(n_remove)) |>
        dplyr::ungroup() |>
        dplyr::select(date, all_of(stratification_names), !!observable) |>
        dplyr::collect()

      # Ensure date is of type Date
      data <- data |>
        dplyr::mutate(date = as.Date(date))


      # Clean up
      DBI::dbRemoveTable(self %.% target_conn, SCDB::id(out))
      DBI::dbRemoveTable(self %.% target_conn, SCDB::id(t_add))
      DBI::dbRemoveTable(self %.% target_conn, SCDB::id(t_remove))

      return(data)
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


    #' @field available_features (`character`)\cr
    #'   A list of available features in the feature store. Read only.
    available_features = purrr::partial(
      .f = active_binding,
      name = "available_features",
      expr = return(names(self$ds_map))
    ),


    #' @field label (`character`)\cr
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
    .slice_ts   = lubridate::today(),

    .ds_map     = NULL, # Must be implemented in child classes
    ds_key_map  = NULL, # Must be implemented in child classes



    verbose = TRUE,

    # Determine which dates are not already computed
    # @description
    #   This method parses the `message` fields of the logs related to `target_table` on the date given by `slice_ts`.
    #   These messages contains information on the date-ranges that are already computed.
    #   Once parsed, the method compares the computed date-ranges with the requested date-range.
    # @param target_table (`character` or `Id`)\cr
    #   The feature table to investigate
    # @param start_date `r rd_start_date()`
    # @param end_date `r rd_end_date()`
    # @param slice_ts `r rd_slice_ts()`
    # @return (`tibble`)\cr
    #   A data frame containing continuous un-computed date-ranges
    #' @importFrom zoo as.Date
    determine_new_ranges = function(target_table, start_date, end_date, slice_ts) {

      if (inherits(target_table, "Id")) {
        target_table <- paste(
          c(purrr::pluck(target_table, "schema"),
            purrr::pluck(target_table, "table")
          ),
          collapse = "."
        )
      }

      # Get a list of the logs for the target_table on the slice_ts
      logs <- dplyr::tbl(self %.% target_conn,
                         SCDB::id(paste(self %.% target_schema, "logs", sep = "."), self %.% target_conn),
                         check_from = FALSE) |>
        dplyr::collect() |>
        tidyr::unite("target_table", "schema", "table", sep = ".", na.rm = TRUE) |>
        dplyr::filter(.data$target_table == !!as.character(target_table), .data$date == !!slice_ts)

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
        dplyr::filter(ds_start_date <= end_date, start_date <= ds_end_date, success == TRUE)                            # nolint: redundant_equals_linter. required for dbplyr translations

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
      new_dates <- zoo::as.Date(setdiff(new_interval, ds_dates))
      # setdiff does not preserve type (converts from Date to numeric)
      # it even breaks the type so hard, that we need to supply the origin also (which for some reason is not default)
      # so we use the zoo::as.Date, since this is reasonably configured...

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
    },


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
