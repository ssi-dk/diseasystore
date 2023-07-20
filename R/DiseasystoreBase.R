#' @title diseasystore base handler
#'
#' @description
#'   This `DiseasystoreBase` [R6][R6::R6Class] class forms the basis of all feature stores.
#'   It defines the primary methods of each feature stores as well as all of the public methods.
#' @export
DiseasystoreBase <- R6::R6Class( # nolint: object_name_linter.
  classname = "DiseasystoreBase",

  public = list(

    #' @description
    #'   Creates a new instance of the `DiseasystoreBase` [R6][R6::R6Class] class.
    #' @template start_date
    #' @template end_date
    #' @template slice_ts
    #' @param source_conn \cr
    #'   Used to specify where data is located.
    #'   Can be `DBIConnection` or file path depending on the `diseasystore`.
    #' @param target_conn (`DBIConnection`)\cr
    #'   A database connection to store the computed features in.
    #' @param verbose (`boolean`)\cr
    #'   Boolean that controls enables debugging information.
    #' @return
    #'   A new instance of the `DiseasystoreBase` [R6][R6::R6Class] class.
    initialize = function(start_date = NULL, end_date = NULL, slice_ts = NULL,
                          source_conn = NULL, target_conn = NULL,
                          verbose = TRUE) {

      coll <- checkmate::makeAssertCollection()
      checkmate::assert_date(start_date, null.ok = TRUE, add = coll)
      checkmate::assert_date(end_date, null.ok = TRUE,   add = coll)
      checkmate::assert_character(slice_ts, pattern = r"{\d{4}-\d{2}-\d{2}(<? \d{2}:\d{2}:\d{2})}",
                                  null.ok = TRUE, add = coll)
      checkmate::assert_logical(verbose, add = coll)
      checkmate::reportAssertions(coll)

      # Set internals
      if (!is.null(slice_ts))   private$slice_ts   <- slice_ts
      if (!is.null(start_date)) private$start_date <- start_date
      if (!is.null(end_date))   private$end_date   <- end_date
      private$verbose <- verbose

      # Set the internal paths
      if (is.null(source_conn)) {
        private$source_conn <- list(class(self)[1], NULL) |>
          purrr::map(~ getOption(paste("diseasystore", ., "source_conn", sep = "."))) |>
          purrr::keep(purrr::negate(is.null)) |>
          purrr::pluck(1)
        if (is.null(private %.% source_conn)) stop("source_conn option not defined for ", class(self)[1])
      }


      if (is.null(target_conn)) {
        private$target_conn <- list(class(self)[1], NULL) |>
          purrr::map(~ getOption(paste("diseasystore", ., "target_conn", sep = "."))) |>
          purrr::keep(purrr::negate(is.null)) |>
          purrr::pluck(1)
        if (is.null(private %.% target_conn)) {
          stop("target_conn option not defined for ", class(self)[1])
        } else {
          private$target_conn <- private$target_conn() # Open the connection
        }
      }

      if (inherits(private %.% target_conn, "PqConnection")) {
        private$target_schema <- "fs"
      }

      # Initialize the feature handlers
      private$initialize_feature_handlers()

    },


    #' @description
    #'   Computes, stores, and returns the requested feature for the study period.
    #' @param feature (`character`)\cr
    #'   The name of a feature defined in the feature store.
    #' @template start_date
    #' @template end_date
    #' @template slice_ts
    #' @return
    #'   A tbl_dbi with the requested feature for the study period.
    get_feature = function(feature,
                           start_date = private %.% start_date,
                           end_date   = private %.% end_date,
                           slice_ts   = private %.% slice_ts) {

      # Load the available features
      fs_map <- self %.% fs_map

      # Check that the feature is implemented
      coll <- checkmate::makeAssertCollection()
      checkmate::assert_choice(feature, unlist(fs_map), add = coll)
      checkmate::assert_date(start_date, any.missing = FALSE, add = coll)
      checkmate::assert_date(end_date,   any.missing = FALSE, add = coll)
      checkmate::assert_character(slice_ts, pattern = r"{\d{4}-\d{2}-\d{2}(<? \d{2}:\d{2}:\d{2})}", add = coll)
      checkmate::assert(!is.null(private %.% source_conn), add = coll)
      checkmate::assert(!is.null(private %.% target_conn), add = coll)
      checkmate::reportAssertions(coll)

      # Determine which feature_loader should be called
      feature_loader <- names(fs_map[fs_map == feature])

      # Create log table
      mg_create_logs_if_missing(paste0(private %.% target_schema, "logs", collapse = "."), private %.% target_conn)

      # Determine which dates need to be computed
      target_table <- paste0(c(private %.% target_schema, feature_loader), collapse = ".")
      fs_missing_ranges <- private$determine_new_ranges(target_table = target_table,
                                                        start_date = start_date,
                                                        end_date   = end_date,
                                                        slice_ts   = slice_ts)

      # Inform that we are computing features
      tic <- Sys.time()
      if (private %.% verbose && nrow(fs_missing_ranges) > 0) {
          cat(glue::glue("feature: {feature} needs to be computed on the specified date interval. ",
                         "please wait..."))
      }

      # Call the feature loader on the dates
      purrr::pwalk(fs_missing_ranges |> dplyr::mutate(r_id = dplyr::row_number()), ~ {

        # Compute the feature for the date range
        fs_feature <- do.call(what = purrr::pluck(private, feature_loader) %.% compute,
                              args = list(start_date = ..1, end_date = ..2,
                                          slice_ts = slice_ts, source_conn = private %.% source_conn))

        # Check it table is copied to target DB
        if (!inherits(fs_feature, "tbl_dbi") ||
          !identical(private %.% source_conn, private %.% target_conn)) {
          fs_feature <- dplyr::copy_to(private %.% target_conn, fs_feature, "fs_tmp", overwrite = TRUE)
        }

        # Add the existing computed data for given slice_ts
        if (mg_table_exists(private %.% target_conn, target_table)) {
          fs_updated_feature <- dplyr::union(
            mg_get_table(private %.% target_conn, target_table, slice_ts = slice_ts),
            fs_feature)
        } else {
          fs_updated_feature <- fs_feature
        }

        # Commit to DB
        capture.output(
          mg_update_snapshot(.data = fs_updated_feature,
                             conn = private %.% target_conn,
                             db_table = target_table,
                             timestamp = slice_ts,
                             message = glue::glue("fs-range: {start_date} - {end_date}"),
                             log_path = NULL, # no log file, but DB logging still enabled
                             log_db = paste0(private %.% target_schema, "logs", collapse = "."),
                             enforce_chronological_order = FALSE))
      })

      # Inform how long has elapsed for updating data
      if (private$verbose && nrow(fs_missing_ranges) > 0) {
        cat(glue::glue("feature: {feature} updated ",
                       "(elapsed time {format(round(difftime(Sys.time(), tic)),2)})."))
      }

      # Finally, return the data to the user
      out <- do.call(what = purrr::pluck(private, feature_loader) %.% get,
                     args = list(target_table = target_table,
                                 slice_ts = slice_ts, target_conn = private %.% target_conn))

      # We need to slice to the period of interest.
      # to ensure proper conversion of variables, we first copy the limits over and then do an inner_join
      dplyr::inner_join(out,
                        data.frame(valid_from = start_date, valid_until = end_date) %>%
                          dplyr::copy_to(private %.% target_conn, ., "fs_tmp", overwrite = TRUE),
                        sql_on = '"LHS"."valid_from" <= "RHS"."valid_until" AND
                                  ("LHS"."valid_until" > "RHS"."valid_from" OR "LHS"."valid_until" IS NULL)',
                        suffix = c("", ".p")) |>
        dplyr::select(!c("valid_from.p", "valid_until.p"))

    },

    #' @description
    #'   Joins various features from feature store assuming a primary feature (observable)
    #'   that contains keys to witch the secondary features (defined by aggregation) can be joined.
    #' @param observable (`character`)\cr
    #'   The name of a feature defined in the feature store
    #' @param aggregation (`list`(`quosures`))\cr
    #'   Expressions in aggregation evaluated to find appropriate features.
    #'   These are then joined to the observable feature before aggregation is performed.
    #' @return
    #'   A tbl_dbi with the requested joined features for the study period.
    key_join_features = function(observable, aggregation) {

      # Store the fs_map
      fs_map <- self %.% fs_map

      # Determine which features are affected by an aggregation
      if (!is.null(aggregation)) {

        # Create regex detection for features
        fs_map_regex <- paste0(r"{(?<=^|\W)}", fs_map, r"{(?=$|\W)}")

        # Perform detection of features in the aggregation
        aggregation_features <- purrr::map(aggregation, rlang::as_label) |>
          purrr::map(\(e) unlist(fs_map[purrr::map_lgl(fs_map_regex, ~ stringr::str_detect(e, .x))])) |>
          unlist() |>
          unique()

        # Report if aggregation not found
        if (is.null(aggregation_features)) {
          err <- glue::glue("Aggregation variable not found. ",
                            "Available aggregation variables are: ",
                            "{toString(fs_map[!startsWith(unlist(fs_map), 'n_')])}")
          private$lg$error(err)
          stop(err)
        }

        aggregation_names <- purrr::map(aggregation, rlang::as_label)
        aggregation_names <- purrr::map2_chr(aggregation_names,
                                             names(aggregation_names),
                                             ~ ifelse(.y == "", .x, .y)) |>
          unname()

        # Fetch requested aggregation features from the feature store
        aggregation_data <- aggregation_features |>
          unique() |>
          purrr::map(
            ~ self$get_feature(.x) |>
              dplyr::mutate(valid_from  = pmax(valid_from, # Simplify interlacing
                                               start_date, na.rm = TRUE),
                            valid_until = pmin(valid_until,
                                               as.Date(as.Date(end_date) + lubridate::days(1)), na.rm = TRUE)))
      } else {
        aggregation_features <- NULL
        aggregation_names <- NULL
        aggregation_data <- NULL
      }

      # Fetch the requested observable from the feature store
      observable_data <- self$get_feature(observable) |>
        dplyr::mutate(valid_from  = pmax(valid_from,  start_date, na.rm = TRUE), # Simplify interlacing
                      valid_until = pmin(valid_until, as.Date(as.Date(end_date) + lubridate::days(1)), na.rm = TRUE))

      # Determine the keys
      observable_keys  <- colnames(dplyr::select(observable_data, tidyselect::starts_with("key_")))

      # Map aggregation_data to observable_keys (if not already)
      if (!is.null(aggregation_data)) {
        aggregation_keys <- purrr::map(aggregation_data, ~ colnames(dplyr::select(., tidyselect::starts_with("key_"))))

        aggregation_data <- aggregation_data |>
          purrr::map_if(!purrr::map_lgl(aggregation_keys, ~ any(observable_keys %in% .)),
                        ~ .) # TODO: create the mapping
      }

      # Merge and prepare for counting
      out <- truncate_interlace(observable_data, aggregation_data) |>
        private$key_join_filter(aggregation_features) |>
        dplyr::compute() |>
        dplyr::group_by(!!!aggregation)

      # Retrieve the aggregators (and ensure they work together)
      key_join_aggregators <- c(purrr::pluck(private, names(fs_map[fs_map == observable])) %.% key_join,
                                purrr::map(aggregation_features,
                                           ~ purrr::pluck(private, names(fs_map)[fs_map == .x]) %.% key_join))

      if (length(unique(key_join_aggregators)) > 1) {
        stop("Aggregation features have different aggregators. Cannot combine.")
      }

      key_join_aggregator <- purrr::pluck(key_join_aggregators, 1)

      # Add the new valid counts
      t_add <- out |>
        dplyr::group_by(date = valid_from, .add = TRUE) |>
        key_join_aggregator(observable) |>
        dplyr::rename(n_add = n) |>
        dplyr::compute()

      # Add the new invalid counts
      t_remove <- out |>
        dplyr::group_by(date = valid_until, .add = TRUE) |>
        key_join_aggregator(observable) |>
        dplyr::rename(n_remove = n) |>
        dplyr::compute()

      # Get all combinations to merge onto
      all_dates <- tibble::tibble(date = seq.Date(from = start_date, to = end_date, by = 1))

      if (!is.null(aggregation)) {
        all_combi <- out |>
          dplyr::distinct(!!!aggregation) |>
          dplyr::full_join(all_dates, by = character(), copy = TRUE) |>
          dplyr::compute()
      } else {
        all_combi <- all_dates
      }

      # Aggregate across dates
      data <- t_add |>
        mg_right_join(all_combi, by = "date", na_by = aggregation_names, copy = is.null(aggregation)) |>
        mg_left_join(t_remove,  by = "date", na_by = aggregation_names) |>
        tidyr::replace_na(list(n_add = 0, n_remove = 0)) |>
        dplyr::group_by(tidyselect::across(tidyselect::all_of(aggregation_names))) |>
        dbplyr::window_order(date) |>
        dplyr::mutate(date, !!observable := cumsum(n_add) - cumsum(n_remove)) |>
        dplyr::ungroup() |>
        dplyr::select(date, all_of(aggregation_names), !!observable) |>
        dplyr::collect()

      return(data)
    }
  ),

  active = list(

    #' @field fs_map (`named list`(`character`))\cr
    #'   A list that maps features known by the feature store to the corresponding feature handlers
    #'   that compute the features.
    fs_map = function() {

      # Generic features are named generic_ in the db
      fs_generic <- private %.% fs_generic
      if (!is.null(fs_generic)) names(fs_generic) <- paste("generic", names(fs_generic), sep = "_")

      # Specific features are named by the case definition of the feature store
      fs_specific <- private %.% fs_specific
      if (!is.null(fs_specific)) {

        # We need to transform case definition to snake case
        fs_case_definition <- private %.% case_definition |>
          stringr::str_to_lower() |>
          stringr::str_replace_all(" ", "_") |>
          stringr::str_replace_all("-", "_")


        # Then we can paste it together
        names(fs_specific) <- names(fs_specific) |>
          purrr::map_chr(~ glue::glue_collapse(sep = "_",
                                               x = c(fs_case_definition, .x)))
      }

      return(c(fs_generic, fs_specific))
    },


    #' @field available_features (`character`)\cr
    #'   A list of available features in the feature store
    available_features = function() {
      return(unlist(self$fs_map, use.names = FALSE))
    }
  ),

  private = list(

    case_definition = NULL,

    start_date = NULL,
    end_date   = NULL,
    slice_ts   = glue::glue("{lubridate::today() - lubridate::days(1)} 09:00:00"),

    fs_generic  = NULL, # Must be implemented in child classes
    fs_specific = NULL, # Must be implemented in child classes
    fs_key_map  = NULL, # Must be implemented in child classes

    target_conn   = NULL, # Must be implemented in child classes
    source_conn   = NULL, # Must be implemented in child classes
    target_schema = NULL,

    verbose = TRUE,

    determine_new_ranges = function(target_table, start_date, end_date, slice_ts) {

      # Get a list of the logs for the target_table on the slice_ts
      logs <- dplyr::tbl(private %.% target_conn,
                         mg_id(paste0(private %.% target_schema, "logs", collapse = "."), private %.% target_conn)) |>
        dplyr::collect() |>
        tidyr::unite("target_table", "schema", "table", sep = ".", na.rm = TRUE) |>
        dplyr::filter(target_table == !!target_table, date == !!slice_ts)

      # If no logs are found, we need to compute on the entire range
      if (nrow(logs) == 0) {
        return(tibble::tibble(start_date = start_date, end_date = end_date))
      }

      # Determine the date ranges used
      logs <- logs |>
        dplyr::mutate(fs_start_date = stringr::str_extract(message, "(?<=fs-range: )([0-9]{4}-[0-9]{2}-[0-9]{2})"),
                      fs_end_date   = stringr::str_extract(message, "([0-9]{4}-[0-9]{2}-[0-9]{2})$")) |>
        dplyr::mutate(across(.cols = c("fs_start_date", "fs_end_date"), .fns = as.Date))

      # Find updates that overlap with requested range
      logs <- logs |>
        dplyr::filter(fs_start_date < end_date, start_date <= fs_end_date)

      # Looks for updates that (potentially) are ongoing
      potentially_ongoing <- logs |>
        dplyr::mutate(duration = as.numeric(difftime(Sys.time(), start_time, unit = "mins"))) |>
        dplyr::filter(is.na(success) & duration < 30) |>
        dplyr::select(message, duration)

      if (nrow(potentially_ongoing) > 0) {
        err <- glue::glue("db: {target_table} is potentially being updated on the specified date interval. ",
                         "Aborting...")
        cat(err)

        potentially_ongoing |>
          purrr::pmap(~ mg_printr(glue::glue("{..1} started updating {round(..2)} minutes ago. ",
                                              "Releasing lock after 30 minutes")))
        stop(err)
      }

      # Determine the dates covered on this slice_ts
      if (mg_nrow(logs) > 0) {
        fs_dates <- logs |>
          dplyr::select(fs_start_date, fs_end_date) |>
          purrr::pmap(\(fs_start_date, fs_end_date) seq.Date(from = as.Date(fs_start_date),
                                                             to = as.Date(fs_end_date),
                                                             by = "1 day")) |>
          purrr::reduce(dplyr::union_all) |> # union does not preserve type (converts from Date to numeric)
          unique() # so we have to use union_all (preserves type) followed by unique (preserves type)
      } else {
        fs_dates <- list()
      }

      # Define the new dates to compute
      new_interval <- seq.Date(from = as.Date(start_date), to = as.Date(end_date), by = "1 day")

      # Determine the dates that needs to be computed
      new_dates <- zoo::as.Date(setdiff(new_interval, fs_dates))
      # setdiff does not preserve type (converts from Date to numeric)
      # it even breaks the type so hard, that we need to supply the origin also (which for some reason is not default)
      # so we use the zoo::as.Date, since this is reasonably configured...

      # Reduce to single intervals
      new_ranges <- tibble::tibble(date = new_dates) |>
        dplyr::mutate(diff = as.numeric(difftime(date, dplyr::lag(date), units = "days"))) |>
        dplyr::filter(is.na(diff) | diff > 1 | dplyr::row_number() == dplyr::n()) |>
        dplyr::transmute(start_date = date, end_date = dplyr::lead(date)) |>
        dplyr::filter(dplyr::if_all(.cols = tidyselect::everything(), .fns = ~!is.na(.)))

      return(new_ranges)
    }
  ),
)

# Set default options for the package related to the diseasystores
options(diseasystore.source_conn = NULL)
options(diseasystore.target_conn = NULL)
