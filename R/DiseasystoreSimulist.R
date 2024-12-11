#' @title feature store handler of synthetic `simulist` features
#'
#' @description
#'   This `DiseasystoreSimulist` [R6][R6::R6Class] brings support for individual level data.
#' @examplesIf requireNamespace("duckdb", quietly = TRUE)
#'   ds <- DiseasystoreSimulist$new(
#'     source_conn = ".",
#'     target_conn = DBI::dbConnect(duckdb::duckdb())
#'   )
#'
#'   rm(ds)
#' @return
#'   A new instance of the `DiseasystoreSimulist` [R6][R6::R6Class] class.
#' @export
#' @importFrom R6 R6Class
DiseasystoreSimulist <- R6::R6Class(                                                                                    # nolint: object_name_linter.
  classname = "DiseasystoreSimulist",
  inherit = DiseasystoreBase,

  public = list(
    #' @description
    #'   Creates a new instance of the `DiseasystoreSimulist` [R6][R6::R6Class] class.
    #' @param ...
    #'   Arguments passed to the `?DiseasystoreBase` constructor.
    #' @return
    #'   A new instance of the `DiseasystoreSimulist` [R6][R6::R6Class] class.
    initialize = function(...) {
      super$initialize(...)

      # We do not support SQLite for this diseasystore since it has poor support for date operations
      checkmate::assert_disjunct(class(self$target_conn), c("SQLiteConnection", "Microsoft SQL Server"))

      private$.max_end_date <- simulist_data |>
        dplyr::select(dplyr::starts_with("date_")) |>
        purrr::reduce(c) |>
        max(na.rm = TRUE)
    }
  ),

  private = list(
    .ds_map = list(
      "birth"       = "simulist_birth",
      "age"         = "simulist_age",
      "sex"         = "simulist_sex",
      "n_positive"  = "simulist_positive",
      "n_admission" = "simulist_admission",
      "n_hospital"  = "simulist_hospital"
    ),
    .label = "Simulist Synthetic Data",

    .min_start_date = as.Date("2019-12-01"),
    .max_end_date = NULL,



    # The "birth" feature contains the birth dates of the individuals and is used later to compute the age
    # of the individuals at any given time.
    simulist_birth = FeatureHandler$new(
      compute = function(start_date, end_date, slice_ts, source_conn, ...) {

        # The simulist data does not generate a birth date, so we generate a synthetic birth date for each individual
        out <- simulist_data |>
          dplyr::transmute(
            "key_pnr" = .data$id,
            "birth" = .data$birth,
            "valid_from" = .data$birth,
            "valid_until" = .data$date_death + lubridate::days(1)
          ) |>
          dplyr::filter({{ start_date }} < .data$valid_until, .data$valid_from <= {{ end_date }})

        return(out)
      },
      key_join = key_join_count
    ),


    # The "age" feature computes the age of the individuals throughout the study period
    simulist_age = FeatureHandler$new(
      compute = function(start_date, end_date, slice_ts, source_conn, ds, ...) {

        # Using birth date, compute the age at the start of the study period
        age <- ds$get_feature("birth", start_date, end_date, slice_ts) |>
          dplyr::mutate(age_at_start = as.integer(!!age_on_date("birth", start_date, conn = ds %.% target_conn))) |>
          dplyr::compute()

        # Now, compute the next birthdays of the individual (as many as we need to cover the study period)
        # and compute the age of the individuals throughout the study period with their birthdays denoting the starts
        # and ends of the validity periods.
        out <- purrr::map(
          seq.int(from = 0, to = ceiling(lubridate::interval(start_date, end_date) / lubridate::years(1))),
          ~ age |>
            dplyr::mutate(
              "age" = .data$age_at_start + .x # The age for this iteration of the age computation loop
            ) |>
            dplyr::mutate( # NOTE, We need to split the mutates to make the "age" column available for the next mutate
              "birthday" = !!add_years("birth", "age", conn = ds %.% target_conn) # Compute the birthday for the age
            ) |>
            dplyr::mutate( # NOTE: Again, we need the split to make "birthday" available for the next mutate
              "next_birthday" = !!add_years("birthday", 1, conn = ds %.% target_conn) # And when that age is not valid
            ) |>
            dplyr::filter( # Now we remove the birthdays that fall outside of the study period
              .data$birthday <= {{ end_date }},
              .data$birthday < .data$valid_until | is.na(.data$valid_until)
            ) |>
            dplyr::transmute( # We assign the birth dates as the validity periods
              "key_pnr" = .data$key_pnr,
              "age" = .data$age,
              "valid_from" = .data$birthday,
              "valid_until" = pmin(.data$valid_until, .data$next_birthday, na.rm = TRUE)
            )
        ) |>
          purrr::reduce(dplyr::union_all) # And we combine each age computation to a single age dataset

        return(out)
      },
      key_join = key_join_count
    ),


    # The "sex" feature simply stores the sex from the simulist data
    simulist_sex = FeatureHandler$new(
      compute = function(start_date, end_date, slice_ts, source_conn, ds, ...) {

        out <- simulist_data |>
          dplyr::right_join( # Join with birth data to validity period
            ds$get_feature("birth", start_date, end_date, slice_ts),
            by = c("id" = "key_pnr"),
            copy = TRUE
          ) |>
          dplyr::transmute(
            "key_pnr" = .data$id,
            "sex" = dplyr::if_else(.data$sex == "m", "Male", "Female"),
            .data$valid_from, .data$valid_until # Use values from birth feature
          )

        # No need to filter to ensure the data is only for the requested time period.
        # Since we right join with the birth feature, the validity period is already filtered.

        return(out)
      },
      key_join = key_join_count
    ),


    # The "n_positive" feature contains the positive tests taken by the individuals
    simulist_positive = FeatureHandler$new(
      compute = function(start_date, end_date, slice_ts, source_conn, ...) {

        out <- simulist_data |>
          dplyr::filter(.data$case_type == "confirmed") |>
          dplyr::transmute(
            "key_pnr" = .data$id,
            "valid_from" = .data$date_onset,
            "valid_until" = .data$valid_from + lubridate::days(1)
          ) |>
          dplyr::filter({{ start_date }} < .data$valid_until, .data$valid_from <= {{ end_date }})


        return(out)
      },
      key_join = key_join_count
    ),


    # The "n_hospital" feature contains the hospitalizations of the individuals
    simulist_hospital = FeatureHandler$new(
      compute = function(start_date, end_date, slice_ts, source_conn, ds, ...) {

        out <- simulist_data |>
          dplyr::filter(.data$case_type == "confirmed", !is.na(.data$date_admission)) |>
          dplyr::transmute(
            "key_pnr" = .data$id,
            "valid_from" = .data$date_admission,
            "valid_until" = .data$date_discharge + lubridate::days(1)
          ) |>
          dplyr::filter({{ start_date }} < .data$valid_until, .data$valid_from <= {{ end_date }})

        return(out)
      },
      key_join = key_join_count
    ),


    # The "n_admission" feature contains the admissions of the individuals
    # We here use the "n_hospital" feature to compute the admissions since the admission is an entry for the
    # first date of hospitalisation
    simulist_admission = FeatureHandler$new(
      compute = function(start_date, end_date, slice_ts, source_conn, ds, ...) {

        out <- ds$get_feature("n_hospital", start_date, end_date, slice_ts) |>
          dplyr::mutate("valid_until" = .data$valid_from + 1L) |>
          dplyr::filter({{ start_date }} < .data$valid_until) # valid_from filtered in n_hospital

        return(out)
      },
      key_join = key_join_count
    )
  )
)
