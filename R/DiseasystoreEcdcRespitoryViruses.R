#' @title feature store handler of EU-ECDC Respiratory viruses features
#'
#' @description
#'   This `DiseasystoreEcdcRespitoryViruses` [R6][R6::R6Class] brings support for using the EU-ECDC
#'   Respiratory viruses weekly data repository.
#'   See the vignette("diseasystore-flu-forecasting-hub") for details on how to configure the feature store
#' @examples
#'   ds <- DiseasystoreEcdcRespitoryViruses$new(
#'     source_conn = ".",
#'     target_conn = DBI::dbConnect(RSQLite::SQLite())
#'   )
#'
#'   rm(ds)
#' @return
#'   A new instance of the `DiseasystoreEcdcRespitoryViruses` [R6][R6::R6Class] class.
#' @export
#' @importFrom R6 R6Class
DiseasystoreEcdcRespitoryViruses <- R6::R6Class(                                                                        # nolint: object_name_linter.
  classname = "DiseasystoreEcdcRespitoryViruses",
  inherit = DiseasystoreBase,

  private = list(
    .ds_map = list(
      "iliari_rates" = "ecdc_respitory_viruses_iliari_rates",
      "age_group"    = "ecdc_respitory_viruses_iliari_rates"
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
        out <- source_conn_github(source_conn, glue::glue("data/snapshots/{as.Date(slice_ts)}_ILIARIRates.csv")) |>
          readr::read_csv(n_max = getOption("diseasystore.DiseasystoreEcdcRespitoryViruses.n_max", default = Inf),
                          show_col_types = FALSE) #|>

        out |>
          dplyr::transmute("key_location" = .data$countryname,
                           "yearweek" = paste(.data$yearweek, 1, sep = "-"),
                           "age_group" = .data$age,
                           "rate" = .data$value) |>
          dplyr::mutate("age_group" = dplyr::case_when(
            .data$age_group == "0-4" ~ "00-04",
            .data$age_group == "5-14" ~ "00-14",
            .data$age_group == "total" ~ NA,
            TRUE ~ .data$age_group)
          ) |>
          dplyr::mutate(valid_from = ISOweek::ISOweek2date(.data$yearweek),
                        valid_until = valid_from + lubridate::days(7)) |>
          dplyr::select(!"yearweek")

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
  options("diseasystore.DiseasystoreEcdcRespitoryViruses.remote_conn" =
   "https://api.github.com/repos/EU-ECDC/Respiratory_viruses_weekly_data")
  options("diseasystore.DiseasystoreEcdcRespitoryViruses.source_conn" =
   "https://api.github.com/repos/EU-ECDC/Respiratory_viruses_weekly_data")
  options("diseasystore.DiseasystoreEcdcRespitoryViruses.target_conn" = "")
  options("diseasystore.DiseasystoreEcdcRespitoryViruses.target_schema" = "")
})
