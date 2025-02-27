if (!rlang::is_installed("simulist")) {
  message(
    "Generation of this data set requires the `simulist` package. ",
    "However as it is not released on CRAN we cannot include the it as a suggested package ",
    "and you need to manually install the package."
  )
}

if (rlang::is_installed("simulist") && rlang::is_installed("usethis")) {

  # Configure `simulist` using their example configuration.
  # The Get Started configuration does not work with the current versions of epiparameter and simulist.

  # Generate the full simulist
  withr::local_seed(1)

  simulist_data <- simulist::sim_linelist(                                                                              # nolint: namespace_linter. simulist is not installed by default
    contact_distribution = function(x) stats::dpois(x = x, lambda = 2),
    infectious_period = function(x) stats::rlnorm(n = x, meanlog = 2, sdlog = 0.5),
    prob_infection = 0.75,
    onset_to_hosp = function(x) stats::rlnorm(n = x, meanlog = 1.5, sdlog = 0.5),
    onset_to_death = function(x) stats::rlnorm(n = x, meanlog = 2.5, sdlog = 0.5),
    onset_to_recovery = NULL,
    hosp_risk = 0.5,
    hosp_death_risk = 0.5,
    non_hosp_death_risk = 0.05,
    outbreak_start_date = as.Date("2019-12-01"),
    outbreak_size = c(1e3, 1e4),
    anonymise = TRUE,
    population_age = c(1, 90),
    case_type_probs = c(suspected = 0.2, probable = 0.3, confirmed = 0.5),
    config = simulist::create_config()                                                                                  # nolint: namespace_linter. simulist is not installed by default
  )

  # Convert to tibble
  simulist_data <- tibble::as_tibble(simulist_data)

  # Convert dates from Date to Date (sic!)
  # ... Somehow R has stored extra information in the dates that makes them different despite being the same date
  simulist_data <- simulist_data |>
    dplyr::mutate(dplyr::across(tidyselect::starts_with("date_"), ~ as.Date(as.character(.))))

  # Generate a "random" birth date for each individual
  simulist_data <- simulist_data |>
    dplyr::mutate("birth" = .data$date_onset - .data$id %% 365 - .data$age * 365, .before = "age")

  # Convert "outcome" to death and recovery
  simulist_data <- simulist_data |>
    dplyr::mutate(
      "date_discharge" = dplyr::case_when(
        .data$outcome == "recovered" & !is.na(.data$date_admission) ~ pmax(.data$date_outcome, .data$date_admission),
        .data$outcome == "died" ~ .data$date_outcome,
        TRUE ~ NA
      ),
      "date_death" = dplyr::case_when(
        .data$outcome == "died" ~ .data$date_outcome,
        TRUE ~ NA
      )
    ) |>
    dplyr::select(!dplyr::any_of(c("outcome", "date_outcome")))

  # Some dates are out of possible chronological order
  simulist_data <- simulist_data |>
    dplyr::mutate(
      "date_discharge" = pmax(.data$date_discharge, .data$date_admission),
      "date_death"     = pmax(.data$date_death, .data$date_admission)
    )

  # Remove surplus information
  simulist_data <- simulist_data |>
    dplyr::select(!dplyr::any_of(c("case_name", "date_first_contact", "date_last_contact", "ct_value")))

  # Store the simulated line list
  usethis::use_data(simulist_data, overwrite = TRUE)
}
