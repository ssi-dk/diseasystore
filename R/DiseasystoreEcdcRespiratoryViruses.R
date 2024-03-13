#' @title feature store handler of EU-ECDC Respiratory viruses features
#'
#' @description
#'   This `DiseasystoreEcdcRespiratoryViruses` [R6][R6::R6Class] brings support for using the EU-ECDC
#'   Respiratory viruses weekly data repository.
#'   See the vignette("diseasystore-ecdc-respiratory-viruses") for details on how to configure the feature store.
#' @examples
#'   ds <- DiseasystoreEcdcRespiratoryViruses$new(
#'     source_conn = ".",
#'     target_conn = DBI::dbConnect(RSQLite::SQLite())
#'   )
#'
#'   rm(ds)
#' @return
#'   A new instance of the `DiseasystoreEcdcRespiratoryViruses` [R6][R6::R6Class] class.
#' @export
#' @importFrom R6 R6Class
DiseasystoreEcdcRespiratoryViruses <- R6::R6Class(                                                                      # nolint: object_name_linter, object_length_linter
  classname = "DiseasystoreEcdcRespiratoryViruses",
  inherit = DiseasystoreBase,

  private = list(
    .ds_map = list(
      "iliari_rates"   = "ecdc_respitory_viruses_iliari_rates",
      "infection_type" = "ecdc_respitory_viruses_iliari_rates",
      "age_group"      = "ecdc_respitory_viruses_iliari_rates"
    ),
    .label = "ECDC Respitory Viruses",


    ecdc_respitory_viruses_iliari_rates = FeatureHandler$new(
      compute = function(start_date, end_date, slice_ts, source_conn) {
        coll <- checkmate::makeAssertCollection()
        checkmate::assert_date(start_date, lower = as.Date("2020-01-01"), add = coll)
        checkmate::assert_date(end_date,   upper = as.Date("2022-09-15"), add = coll)
        checkmate::assert_character(source_conn, len = 1, add = coll)
        checkmate::reportAssertions(coll)

        # Load and parse
        out <- source_conn_github(
          source_conn, glue::glue("data/snapshots/{as.Date(slice_ts)}_ILIARIRates.csv"),
          pull = diseasyoption("pull", "DiseasystoreEcdcRespiratoryViruses", .default = TRUE)
        ) |>
          readr::read_csv(n_max = diseasyoption("n_max", "DiseasystoreEcdcRespiratoryViruses", default = Inf),
                          show_col_types = FALSE) |>
          dplyr::transmute(
            "key_location" = .data$countryname,
            "age_group" = dplyr::case_when(
              .data$age == "0-4" ~ "00-04",
              .data$age == "5-14" ~ "05-14",
              .data$age == "total" ~ NA,
              TRUE ~ .data$age
            ),
            "infection_type" = dplyr::case_when(
              .data$indicator == "ILIconsultationrate" ~ "ILI",
              .data$indicator == "ARIconsultationrate" ~ "ARI",
              TRUE ~ NA
            ),
            "rate" = .data$value,
            "valid_from" = ISOweek::ISOweek2date(paste(.data$yearweek, 1, sep = "-")),
            "valid_until" = .data$valid_from + lubridate::days(7)
          )

        return(out)
      },
      key_join = \(.data, feature) .data
    ),


    key_join_filter = function(.data, stratification_features,
                               start_date = private %.% start_date,
                               end_date = private %.% end_date) {

      # The EU-ECDC data contains surplus data depending for the age groups.
      # Ie. individual age_groups are included as well as the total.
      # We need to filter at the appropriate stratification level when these doubly counted
      # features are requested.

      # Manually perform filtering
      if ("age_group" %in% stratification_features) {

        return(dplyr::filter(.data, !is.na(.data$age_group)))

      } else {

        return(dplyr::filter(.data, !is.na(.data$age_group)))

      }
    }
  )
)


# Set default options for the package related to the Google COVID-19 store
rlang::on_load({
  options(
    "diseasystore.DiseasystoreEcdcRespiratoryViruses.remote_conn" =
      "https://api.github.com/repos/EU-ECDC/Respiratory_viruses_weekly_data"
  )
  options(
    "diseasystore.DiseasystoreEcdcRespiratoryViruses.source_conn" =
      "https://api.github.com/repos/EU-ECDC/Respiratory_viruses_weekly_data"
  )
  options("diseasystore.DiseasystoreEcdcRespiratoryViruses.target_conn" = "")
  options("diseasystore.DiseasystoreEcdcRespiratoryViruses.target_schema" = "")
  options("diseasystore.DiseasystoreEcdcRespiratoryViruses.pull" = TRUE)
})
