#' `FeatureHandler` factory for Google COVID-19 epidemic metrics
#'
#' @description
#'   This function implements a `?FeatureHandler` factory for Google COVID-19 epidemic metrics.
#'   This factory is used when defining the `DiseasystoreGoogleCovid19` feature store.
#' @param google_pattern (`character`)\cr
#'   A regexp pattern that matches Google's naming of the metric in "by-age.csv.gz".
#' @param out_name (`character`)\cr
#'   A the name to store the metric in our our feature store.
#' @return
#'   A new instance of `?FeatureHandler` [R6][R6::R6Class] class corresponding to the epidemic metric.
#' @importFrom rlang .data
#' @noRd
google_covid_19_metric <- function(google_pattern, out_name) {                                                          # nocov start
  FeatureHandler$new(
    compute = function(start_date, end_date, slice_ts, source_conn, ...) {
      checkmate::assert_character(source_conn, len = 1)

      # Load and parse
      data <- source_conn_path(source_conn, "by-age.csv") |>
        readr::read_csv(n_max = diseasyoption("n_max", "DiseasystoreGoogleCovid19", .default = Inf),
                        show_col_types = FALSE) |>
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
}                                                                                                                       # nocov end


#' @title feature store handler of Google Health COVID-19 Open Data features
#'
#' @description
#'   This `DiseasystoreGoogleCovid19` [R6][R6::R6Class] brings support for using the Google
#'   Health COVID-19 Open Data repository.
#'   See the vignette("diseasystore-google-covid-19") for details on how to configure the feature store.
#' @examplesIf requireNamespace("RSQLite", quietly = TRUE)
#'   ds <- DiseasystoreGoogleCovid19$new(
#'     source_conn = ".",
#'     target_conn = DBI::dbConnect(RSQLite::SQLite())
#'   )
#'
#*   ds$available_features
#'
#'   rm(ds)
#' @return
#'   A new instance of the `DiseasystoreGoogleCovid19` [R6][R6::R6Class] class.
#' @export
#' @importFrom R6 R6Class
DiseasystoreGoogleCovid19 <- R6::R6Class(                                                                               # nolint: object_name_linter.
  classname = "DiseasystoreGoogleCovid19",
  inherit = DiseasystoreBase,

  private = list(
    .ds_map = list(
      "n_population"    = "google_covid_19_population",
      "age_group"       = "google_covid_19_age_group",
      "country_id"      = "google_covid_19_index",
      "country"         = "google_covid_19_index",
      "region_id"       = "google_covid_19_index",
      "region"          = "google_covid_19_index",
      "subregion_id"    = "google_covid_19_index",
      "subregion"       = "google_covid_19_index",
      "n_hospital"      = "google_covid_19_hospital",
      "n_deaths"        = "google_covid_19_deaths",
      "n_positive"      = "google_covid_19_positive",
      "n_icu"           = "google_covid_19_icu",
      "n_ventilator"    = "google_covid_19_ventilator",
      "min_temperature" = "google_covid_19_min_temperature",
      "max_temperature" = "google_covid_19_max_temperature"
    ),
    .observables_regex = r"{^n_(?=\w)|(?<=\w)_temperature$}",
    .label = "Google COVID-19",

    .min_start_date = as.Date("2020-01-01"),
    .max_end_date = as.Date("2022-09-15"), # Data source is no longer actively updated

    google_covid_19_population = FeatureHandler$new(
      compute = function(start_date, end_date, slice_ts, source_conn, ...) {
        checkmate::assert_character(source_conn, len = 1)

        # Load and parse
        out <- source_conn_path(source_conn, "demographics.csv") |>
          readr::read_csv(n_max = diseasyoption("n_max", "DiseasystoreGoogleCovid19", .default = Inf),
                          show_col_types = FALSE) |>
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
    ),

    google_covid_19_index = FeatureHandler$new(
      compute = function(start_date, end_date, slice_ts, source_conn, ...) {
        checkmate::assert_character(source_conn, len = 1)

        # Load and parse
        out <- source_conn_path(source_conn, "index.csv") |>
          readr::read_csv(n_max = diseasyoption("n_max", "DiseasystoreGoogleCovid19", .default = Inf),
                          show_col_types = FALSE) |>
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
          dplyr::mutate(
            "region_id" = dplyr::if_else(.data$country_id == .data$region_id, NA, .data$region_id),
            "subregion_id" = dplyr::if_else(.data$key_location != .data$subregion_id, NA, .data$subregion_id),
            "valid_from" = as.Date("2020-01-01"),
            "valid_until" = as.Date(NA)
          )

        return(out)
      },
      key_join = key_join_sum
    ),


    google_covid_19_hospital   = google_covid_19_metric("hospitalized_patients", "n_hospital"),
    google_covid_19_deaths     = google_covid_19_metric("deceased", "n_deaths"),
    google_covid_19_positive   = google_covid_19_metric("confirmed", "n_positive"),
    google_covid_19_icu        = google_covid_19_metric("intensive_care_patients", "n_icu"),
    google_covid_19_ventilator = google_covid_19_metric("ventilator_patients", "n_ventilator"),

    google_covid_19_age_group = FeatureHandler$new(
      compute = function(start_date, end_date, slice_ts, source_conn, ...) {
        checkmate::assert_character(source_conn, len = 1)

        # Load and parse
        out <- source_conn_path(source_conn, "by-age.csv") |>
          readr::read_csv(n_max = diseasyoption("n_max", "DiseasystoreGoogleCovid19", .default = Inf),
                          show_col_types = FALSE)

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
          dplyr::mutate("valid_from" = as.Date("2020-01-01"), "valid_until" = as.Date(NA)) |>
          dplyr::ungroup()

        return(out)
      },
      key_join = key_join_sum
    ),


    google_covid_19_min_temperature  = FeatureHandler$new(
      compute = function(start_date, end_date, slice_ts, source_conn, ...) {
        checkmate::assert_character(source_conn, len = 1)

        # Load and parse
        out <- source_conn_path(source_conn, "weather.csv") |>
          readr::read_csv(n_max = diseasyoption("n_max", "DiseasystoreGoogleCovid19", .default = Inf),
                          show_col_types = FALSE) |>
          dplyr::mutate("date" = as.Date(.data$date)) |>
          dplyr::filter({{ start_date }} <= .data$date, .data$date <= {{ end_date }}) |>
          dplyr::select("key_location" = "location_key",
                        "date",
                        "min_temperature" = "minimum_temperature_celsius") |>
          dplyr::mutate("valid_from" = .data$date, "valid_until" = as.Date(.data$date + lubridate::days(1)))

        return(out)
      },
      key_join = key_join_min
    ),

    google_covid_19_max_temperature  = FeatureHandler$new(
      compute = function(start_date, end_date, slice_ts, source_conn, ...) {
        checkmate::assert_character(source_conn, len = 1)

        # Load and parse
        out <- source_conn_path(source_conn, "weather.csv") |>
          readr::read_csv(n_max = diseasyoption("n_max", "DiseasystoreGoogleCovid19", .default = Inf),
                          show_col_types = FALSE) |>
          dplyr::mutate("date" = as.Date(.data$date)) |>
          dplyr::filter({{ start_date }} <= .data$date, .data$date <= {{ end_date }}) |>
          dplyr::select("key_location" = "location_key",
                        "date",
                        "max_temperature" = "maximum_temperature_celsius") |>
          dplyr::mutate("valid_from" = .data$date, "valid_until" = as.Date(.data$date + lubridate::days(1)))

        return(out)
      },
      key_join = key_join_max
    ),


    key_join_filter = function(.data, stratification_features,
                               start_date = private %.% start_date,
                               end_date = private %.% end_date) {

      # The Google data contains surplus data depending on the stratification.
      # I.e. some individuals are counted more than once.
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
          dplyr::group_by(.data$country_id) |>
          dplyr::slice_min(.data$aggregation_level) |>
          dplyr::ungroup() |>
          dplyr::select("key_location")

        return(dplyr::inner_join(.data, filter_level, by = "key_location", copy = TRUE))

      } else if (purrr::some(stratification_features, ~ . %in% c("country_id", "country"))) {
        return(.data |> dplyr::filter(.data$key_location == .data$country_id))
      } else if (purrr::some(stratification_features, ~ . %in% c("region_id", "region"))) {
        return(.data |> dplyr::filter(.data$key_location == .data$region_id))
      } else if (purrr::some(stratification_features, ~ . %in% c("subregion_id", "subregion"))) {
        return(.data |> dplyr::filter(.data$key_location == .data$subregion_id))
      } else {
        stop("Edge case detected in $key_join_filter() (DiseasyStoreGoogleCovid19)", call. = FALSE)
      }
    }
  )
)


# Set default options for the package related to the Google COVID-19 store
rlang::on_load({
  options("diseasystore.DiseasystoreGoogleCovid19.remote_conn" = "https://storage.googleapis.com/covid19-open-data/v3/")
  options("diseasystore.DiseasystoreGoogleCovid19.source_conn" = "https://storage.googleapis.com/covid19-open-data/v3/")
  options("diseasystore.DiseasystoreGoogleCovid19.target_conn" = "")
  options("diseasystore.DiseasystoreGoogleCovid19.target_schema" = "")
})
