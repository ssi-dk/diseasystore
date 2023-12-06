#' @title feature store handler of Google Health COVID-19 Open Data features
#'
#' @description
#'   This `DiseasystoreGoogleCovid19` [R6][R6::R6Class] brings support for using the Google
#'   Health COVID-19 Open Data repository.
#'   See the vignette("google_covid_19_data") for details on how to configure the feature store
#' @examples
#'   ds <- DiseasystoreGoogleCovid19$new(source_conn = ".",
#'                                       target_conn = DBI::dbConnect(RSQLite::SQLite()))
#'
#' @return
#'   A new instance of the `DiseasystoreGoogleCovid19` [R6][R6::R6Class] class.
#' @export
DiseasystoreGoogleCovid19 <- R6::R6Class( # nolint: object_name_linter.
  classname = "DiseasystoreGoogleCovid19",
  inherit = DiseasystoreBase,

  public = list(

    #' @description
    #'   This function implements an intermediate filtering in the stratification pipeline.
    #'   For semi-aggregated data like Googles COVID-19 data, some people are counted more than once.
    #'   The `key_join_filter` is inserted into the stratification pipeline to remove this double counting.
    #' @param .data `r rd_.data()`
    #' @param stratification_features (`character`)\cr
    #'   A list of the features included in the stratification process.
    #' @param start_date `r rd_start_date()`
    #' @param end_date `r rd_end_date()`
    #' @return
    #'   A subset of `.data` filtered to remove double counting
    key_join_filter = function(.data, stratification_features,
                               start_date = private %.% start_date,
                               end_date = private %.% end_date) {

      # The Google data contains surplus data depending on the stratification.
      # Ie. some individuals are counted more than once.
      # Eg. once at the country level, and then again at the region level etc.
      # We need to filter at the appropriate stratification level when these doubly counted
      # features are requested.

      # Manually perform filtering
      if (is.null(stratification_features) ||
            (!is.null(stratification_features) &&
               purrr::none(stratification_features,
                           ~ . %in% c("country_id", "country",
                                      "region_id", "region",
                                      "subregion_id", "subregion")))) {

        # If no spatial stratification is requested, use the largest available per country
        filter_level <- self$get_feature("country_id", start_date, end_date) |>
          dplyr::group_by(country_id) |>
          dplyr::slice_min(aggregation_level) |>
          dplyr::ungroup() |>
          dplyr::select(key_location)

        return(dplyr::inner_join(.data, filter_level, by = "key_location", copy = TRUE))

      } else if (stratification_features %in% c("country_id", "country")) {
        return(.data |> dplyr::filter(key_location == country_id))
      } else if (stratification_features %in% c("region_id", "region")) {
        return(.data |> dplyr::filter(key_location == region_id))
      } else if (stratification_features %in% c("subregion_id", "subregion")) {
        return(.data |> dplyr::filter(key_location == subregion_id))
      }
    }
  ),

  private = list(
    .ds_map = list("population"      = "n_population",
                   "age_group"       = "age_group",
                   "index"           = "country_id",
                   "index"           = "country",
                   "index"           = "region_id",
                   "index"           = "region",
                   "index"           = "subregion_id",
                   "index"           = "subregion",
                   "hospital"        = "n_hospital",
                   "deaths"          = "n_deaths",
                   "positive"        = "n_positive",
                   "icu"             = "n_icu",
                   "ventilator"      = "n_ventilator",
                   "min_temperature" = "min_temperature",
                   "max_temperature" = "max_temperature"),
    .label = "Google COVID-19",

    google_covid_19_population       = NULL,
    google_covid_19_index            = NULL,
    google_covid_19_hospital         = NULL,
    google_covid_19_deaths           = NULL,
    google_covid_19_positive         = NULL,
    google_covid_19_icu              = NULL,
    google_covid_19_ventilator       = NULL,
    google_covid_19_age_group        = NULL,
    google_covid_19_min_temperature  = NULL,
    google_covid_19_max_temperature  = NULL,

    initialize_feature_handlers = function() {

      # Here we initialize each of the feature handlers for the class
      # See the documentation above at the corresponding methods
      private$google_covid_19_population <- google_covid_19_population_()
      private$google_covid_19_age_group  <- google_covid_19_age_group_()
      private$google_covid_19_index      <- google_covid_19_index_()

      private$google_covid_19_hospital   <- google_covid_19_hospital_()
      private$google_covid_19_deaths     <- google_covid_19_deaths_()
      private$google_covid_19_positive   <- google_covid_19_positive_()
      private$google_covid_19_icu        <- google_covid_19_icu_()
      private$google_covid_19_ventilator <- google_covid_19_ventilator_()

      private$google_covid_19_min_temperature <- google_covid_19_min_temperature_()
      private$google_covid_19_max_temperature <- google_covid_19_max_temperature_()
    }
  )
)


#' `FeatureHandler` factory for Google COIVD-19 epidemic metrics
#'
#' @description
#'   This function implements a `FeatureHandler` factory for Google COIVD-19 epidemic metrics.
#'   This factory is used when defining the `DiseasystoreGoogleCovid19` feature store.
#' @param google_pattern (`character`)\cr
#'   A regexp pattern that matches Googles naming of the metric in "by-age.csv.gz".
#' @param out_name (`character`)\cr
#'   A the name to store the metric in our our feature store.
#' @return
#'   A new instance of `FeatureHandler` [R6][R6::R6Class] class corresponding to the epidemic metric.
#' @importFrom rlang .data
#' @noRd
google_covid_19_metric <- function(google_pattern, out_name) {
  FeatureHandler$new(
    compute = function(start_date, end_date, slice_ts, source_conn) {
      coll <- checkmate::makeAssertCollection()
      checkmate::assert_date(start_date, lower = as.Date("2020-01-01"), add = coll)
      checkmate::assert_date(end_date,   upper = as.Date("2022-09-15"), add = coll)
      checkmate::assert_character(source_conn, len = 1, add = coll)
      checkmate::reportAssertions(coll)

      # Load and parse
      data <- source_conn_path(source_conn, "by-age.csv") |>
        readr::read_csv(n_max = ifelse(testthat::is_testing(), 1000, Inf), show_col_types = FALSE) |>
        dplyr::mutate("date" = as.Date(.data$date)) |>
        dplyr::filter(.data$date >= as.Date("2020-01-01"),
                      {{ start_date }} <= .data$date, .data$date <= {{ end_date }}) |>
        dplyr::select("location_key", "date", tidyselect::starts_with(glue::glue("new_{google_pattern}"))) |>
        tidyr::pivot_longer(!c("location_key", "date"),
                            names_to = c("tmp", "key_age_bin"), names_sep = "_age_",
                            values_to = out_name, values_transform = as.numeric) |>
        dplyr::mutate("valid_from" = .data$date, "valid_until" = as.Date(.data$date + lubridate::days(1))) |>
        dplyr::select(tidyselect::all_of(c("location_key", "key_age_bin", out_name, "valid_from", "valid_until"))) |>
        dplyr::rename("key_location" = "location_key")

      return(data)
    },
    key_join = key_join_sum
  )
}


# The functions below generate `FeatureHandler`s for the feature store.
# by keeping them here as separate files, we can use them in our testing framework to check that everything
# works as expected
google_covid_19_hospital_   <- function() google_covid_19_metric("hospitalized_patients", "n_hospital")
google_covid_19_deaths_     <- function() google_covid_19_metric("deceased", "n_deaths")
google_covid_19_positive_   <- function() google_covid_19_metric("confirmed", "n_positive")
google_covid_19_icu_        <- function() google_covid_19_metric("intensive_care_patients", "n_icu")
google_covid_19_ventilator_ <- function() google_covid_19_metric("ventilator_patients", "n_ventilator")


google_covid_19_population_ <- function() {
  FeatureHandler$new(
    compute = function(start_date, end_date, slice_ts, source_conn) {
      coll <- checkmate::makeAssertCollection()
      checkmate::assert_date(start_date, lower = as.Date("2020-01-01"), add = coll)
      checkmate::assert_date(end_date,   upper = as.Date("2022-09-15"), add = coll)
      checkmate::assert_character(source_conn, len = 1, add = coll)
      checkmate::reportAssertions(coll)

      # Load and parse
      out <- source_conn_path(source_conn, "demographics.csv") |>
        readr::read_csv(n_max = ifelse(testthat::is_testing(), 1000, Inf), show_col_types = FALSE) |>
        dplyr::select("location_key", tidyselect::starts_with("population_age_")) |>
        tidyr::pivot_longer(!"location_key",
                            names_to = c("tmp", "age_group"),
                            names_sep = "_age_",
                            values_to = "n_population") |>
        dplyr::mutate(age_group_lower = stringr::str_extract(.data$age_group, r"{^\d*}"),
                      age_group_upper = stringr::str_extract(.data$age_group, r"{\d*$}"),
                      age_group_sep   = dplyr::if_else(.data$age_group_upper == "", "+", "-")) |>
        tidyr::unite("age_group", "age_group_lower", "age_group_sep", "age_group_upper", sep = "", na.rm = TRUE) |>
        dplyr::select("key_location" = "location_key", "age_group", "n_population") |>
        dplyr::mutate(valid_from = as.Date("2020-01-01"), valid_until = as.Date(NA))

      return(out)
    },
    key_join = key_join_sum
  )
}


google_covid_19_age_group_ <- function() {
  FeatureHandler$new(
    compute = function(start_date, end_date, slice_ts, source_conn) {

      coll <- checkmate::makeAssertCollection()
      checkmate::assert_date(start_date, lower = as.Date("2020-01-01"), add = coll)
      checkmate::assert_date(end_date,   upper = as.Date("2022-09-15"), add = coll)
      checkmate::assert_character(source_conn, len = 1, add = coll)
      checkmate::reportAssertions(coll)

      # Load and parse
      out <- source_conn_path(source_conn, "by-age.csv") |>
        readr::read_csv(n_max = ifelse(testthat::is_testing(), 1000, Inf), show_col_types = FALSE)

      # We need a map between age_bin and age_group
      age_bin_map <- out |>
        dplyr::group_by(.data$location_key) |>
        dplyr::select("location_key", tidyselect::starts_with("age_bin")) |>
        dplyr::distinct() |>
        dplyr::left_join(dplyr::select(out, "location_key", "date", tidyselect::starts_with("age_bin")),
                         by = colnames(dplyr::select(out, "location_key", tidyselect::starts_with("age_bin"))),
                         multiple = "first")

      # Some regions changes age stratification. Discard for now
      age_bin_map <- age_bin_map |>
        dplyr::select("location_key") |>
        dplyr::filter(dplyr::n() == 1) |>
        dplyr::inner_join(age_bin_map, by = "location_key")

      # With these filtered, we map age_bins to age_groups
      age_bin_map <- age_bin_map |>
        dplyr::select("location_key", "date", tidyselect::everything()) |>
        tidyr::pivot_longer(!c("location_key", "date"),
                            names_to = c("tmp", "age_bin"),
                            names_sep = "_bin_",
                            values_to = "age_group") |>
        dplyr::mutate(age_group_lower = as.numeric(stringr::str_extract(.data$age_group, r"{^\d*}"))) |>
        dplyr::group_modify(~ {
          na_bins <- is.na(.x$age_group_lower)
          data.frame(age_bin = .x$age_bin[!na_bins],
                     age_group = age_labels(.x$age_group_lower[!na_bins]))
        })

      # And finally copy to the DB
      out <- age_bin_map |>
        dplyr::rename("key_age_bin" = "age_bin", "key_location" = "location_key") |>
        dplyr::mutate("valid_from" = as.Date("2020-01-01"), "valid_until" = as.Date(NA))

      return(out)
    },
    key_join = key_join_sum
  )
}

google_covid_19_index_ <- function() {
  FeatureHandler$new(
    compute = function(start_date, end_date, slice_ts, source_conn) {
      coll <- checkmate::makeAssertCollection()
      checkmate::assert_date(start_date, lower = as.Date("2020-01-01"), add = coll)
      checkmate::assert_date(end_date,   upper = as.Date("2022-09-15"), add = coll)
      checkmate::assert_character(source_conn, len = 1, add = coll)
      checkmate::reportAssertions(coll)

      # Load and parse
      out <- source_conn_path(source_conn, "index.csv") |>
        readr::read_csv(n_max = ifelse(testthat::is_testing(), 1000, Inf), show_col_types = FALSE) |>
        dplyr::transmute("key_location" = .data$location_key,
                         "country_id"   = .data$country_code,
                         "country"      = .data$country_name,
                         .data$country_code,
                         .data$subregion1_code,
                         "region"       = .data$subregion1_name,
                         "subregion_id" = .data$location_key,
                         "subregion"    = .data$subregion2_name,
                         .data$aggregation_level) |>
        tidyr::unite("region_id", "country_code", "subregion1_code", na.rm = TRUE) |>
        dplyr::mutate(region_id    = dplyr::if_else(.data$country_id == .data$region_id,
                                                    NA, .data$region_id),
                      subregion_id = dplyr::if_else(.data$key_location != .data$subregion_id,
                                                    NA, .data$subregion_id)) |>
        dplyr::mutate("valid_from" = as.Date("2020-01-01"), "valid_until" = as.Date(NA))

      return(out)
    },
    key_join = key_join_sum
  )
}

google_covid_19_min_temperature_ <- function() { # nolint: object_length_linter.
  FeatureHandler$new(
    compute = function(start_date, end_date, slice_ts, source_conn) {
      coll <- checkmate::makeAssertCollection()
      checkmate::assert_date(start_date, lower = as.Date("2020-01-01"), add = coll)
      checkmate::assert_date(end_date,   upper = as.Date("2022-09-15"), add = coll)
      checkmate::assert_character(source_conn, len = 1, add = coll)
      checkmate::reportAssertions(coll)

      # Load and parse
      out <- source_conn_path(source_conn, "weather.csv") |>
        readr::read_csv(n_max = ifelse(testthat::is_testing(), 1000, Inf), show_col_types = FALSE) |>
        dplyr::mutate("date" = as.Date(.data$date)) |>
        dplyr::filter({{ start_date }} <= .data$date, .data$date <= {{ end_date }}) |>
        dplyr::select("key_location" = "location_key",
                      "date",
                      "min_temperature" = "minimum_temperature_celsius") |>
        dplyr::mutate("valid_from" = .data$date, "valid_until" = as.Date(.data$date + lubridate::days(1)))

      return(out)
    },
    key_join = key_join_min
  )
}

google_covid_19_max_temperature_ <- function() { # nolint: object_length_linter.
  FeatureHandler$new(
    compute = function(start_date, end_date, slice_ts, source_conn) {
      coll <- checkmate::makeAssertCollection()
      checkmate::assert_date(start_date, lower = as.Date("2020-01-01"), add = coll)
      checkmate::assert_date(end_date,   upper = as.Date("2022-09-15"), add = coll)
      checkmate::assert_character(source_conn, len = 1, add = coll)
      checkmate::reportAssertions(coll)

      # Load and parse
      out <- source_conn_path(source_conn, "weather.csv") |>
        readr::read_csv(n_max = ifelse(testthat::is_testing(), 1000, Inf), show_col_types = FALSE) |>
        dplyr::mutate("date" = as.Date(.data$date)) |>
        dplyr::filter({{ start_date }} <= .data$date, .data$date <= {{ end_date }}) |>
        dplyr::select("key_location" = "location_key",
                      "date",
                      "max_temperature" = "maximum_temperature_celsius") |>
        dplyr::mutate("valid_from" = .data$date, "valid_until" = as.Date(.data$date + lubridate::days(1)))

      return(out)
    },
    key_join = key_join_max
  )
}



# Set default options for the package related to the Google COVID-19 store
rlang::on_load({
  options("diseasystore.DiseasystoreGoogleCovid19.remote_conn" = "https://storage.googleapis.com/covid19-open-data/v3/")
  options("diseasystore.DiseasystoreGoogleCovid19.source_conn" = "https://storage.googleapis.com/covid19-open-data/v3/")
  options("diseasystore.DiseasystoreGoogleCovid19.target_conn" = "")
  options("diseasystore.DiseasystoreGoogleCovid19.target_schema" = "")
})
