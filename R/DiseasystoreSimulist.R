#' @title feature store handler of synthetic `simulist` features
#'
#' @description
#'   This `DiseasystoreSimulist` [R6][R6::R6Class] brings support for individual level data.
#' @examples
#'   ds <- DiseasystoreSimulist$new(
#'     source_conn = ".",
#'     target_conn = DBI::dbConnect(RSQLite::SQLite())
#'   )
#'
#'   rm(ds)
#' @return
#'   A new instance of the `DiseasystoreSimulist` [R6][R6::R6Class] class.
#' @noRd
#' @importFrom R6 R6Class
DiseasystoreSimulist <- R6::R6Class(                                                                                    # nolint: object_name_linter.
  classname = "DiseasystoreSimulist",
  inherit = DiseasystoreBase,

  private = list(
    .ds_map = list(
      "birth"       = "simulist_birth",
      "age"         = "simulist_age",
      "sex"         = "simulist_sex",
      "n_test"      = "simulist_test",
      "n_positive"  = "simulist_positive",
      "n_admission" = "simulist_admission",
      "n_hospital"  = "simulist_hospital"
    ),
    .label = "Simulist Synthetic Data",

    # The "birth" feature contains the birth dates of the individuals and is used later to compute the age
    # of the individuals at any given time.
    simulist_birth = FeatureHandler$new(
      compute = function(start_date, end_date, slice_ts, source_conn, ...) {
        coll <- checkmate::makeAssertCollection()
        checkmate::assert_date(start_date, lower = as.Date("2019-12-01"), add = coll)
        checkmate::assert_date(end_date,   upper = as.Date("2020-01-13"), add = coll)
        checkmate::reportAssertions(coll)

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
        coll <- checkmate::makeAssertCollection()
        checkmate::assert_date(start_date, lower = as.Date("2019-12-01"), add = coll)
        checkmate::assert_date(end_date,   upper = as.Date("2020-01-13"), add = coll)
        checkmate::reportAssertions(coll)

        # Using birth date, compute the age at the start of the study period
        age <- ds$get_feature("birth", start_date, end_date, slice_ts) |>
          dplyr::mutate(age_at_start = as.integer(!!age_on_date("birth", start_date, conn = ds %.% target_conn))) |>
          dplyr::compute()

        # Now, compute the next birthdates of the individual (as many as we need to cover the study period)
        # and compute the age of the individuals throughout the study period with their birthdates denoting the starts
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
              "next_birthday" = !!add_years("birthday", 1,  conn = ds %.% target_conn) # And when that age is not valid
            ) |>
            dplyr::filter( # Now we remove the birthdays that fall outside of the study period
              .data$birthday <= !!end_date,
              .data$birthday < .data$valid_until | is.na(.data$valid_until)
            ) |>
            dplyr::transmute( # We assign the birthdates as the validity periods
              "key_pnr" = .data$key_pnr,
              "age" = .data$age,
              "valid_from" = as.Date(.data$birthday),
              "valid_until" = pmin(.data$valid_until, as.Date(.data$next_birthday), na.rm = TRUE)
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
          dplyr::left_join(
            ds$get_feature("birth", start_date, end_date, slice_ts),
            by = c("id" = "key_pnr"),
            copy = TRUE
          ) |>
          dplyr::transmute(
            "key_pnr" = .data$id,
            "sex" = dplyr::if_else(.data$sex == "m", "Male", "Female"),
            .data$valid_from, .data$valid_until # Use values from birth feature
          ) |>
          dplyr::filter({{ start_date }} < .data$valid_until, .data$valid_from <= {{ end_date }})

        return(out)
      },
      key_join = key_join_count
    ),


    # The "n_test" feature contains the tests taken by the individuals
    simulist_test = FeatureHandler$new(
      compute = function(start_date, end_date, slice_ts, source_conn, ...) {
        coll <- checkmate::makeAssertCollection()
        checkmate::assert_date(start_date, lower = as.Date("2019-12-01"), add = coll)
        checkmate::assert_date(end_date,   upper = as.Date("2020-01-13"), add = coll)
        checkmate::reportAssertions(coll)

        out <- simulist_data |>
          dplyr::filter(.data$case_type != "suspected") |>
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


    # The "n_positive" feature contains the positive tests taken by the individuals
    simulist_positive = FeatureHandler$new(
      compute = function(start_date, end_date, slice_ts, source_conn, ...) {
        coll <- checkmate::makeAssertCollection()
        checkmate::assert_date(start_date, lower = as.Date("2019-12-01"), add = coll)
        checkmate::assert_date(end_date,   upper = as.Date("2020-01-13"), add = coll)
        checkmate::reportAssertions(coll)

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
        coll <- checkmate::makeAssertCollection()
        checkmate::assert_date(start_date, lower = as.Date("2019-12-01"), add = coll)
        checkmate::assert_date(end_date,   upper = as.Date("2020-01-13"), add = coll)
        checkmate::reportAssertions(coll)

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
        coll <- checkmate::makeAssertCollection()
        checkmate::assert_date(start_date, lower = as.Date("2019-12-01"), add = coll)
        checkmate::assert_date(end_date,   upper = as.Date("2020-01-13"), add = coll)
        checkmate::reportAssertions(coll)

        out <- ds$get_feature("n_hospital", start_date, end_date, slice_ts) |>
          dplyr::mutate("valid_until" = .data$valid_from + 1L) |>
          dplyr::filter({{ start_date }} < .data$valid_until, .data$valid_from <= {{ end_date }})

        return(out)
      },
      key_join = key_join_count
    )
  )
)
