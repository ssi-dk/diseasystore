if (rlang::is_installed("epiparameter") && rlang::is_installed("simulist") && rlang::is_installed("usethis")) {

  # Configure `simulist` using their quick-start configuration.

  # Create COVID-19 contact distribution
  contact_distribution <- epiparameter::epidist(
    disease = "COVID-19",
    epi_dist = "contact distribution",
    prob_distribution = "pois",
    prob_distribution_params = c(mean = 5)
  )

  # Create COVID-19 infectious period
  infectious_period <- epiparameter::epidist(
    disease = "COVID-19",
    epi_dist = "contact interval",
    prob_distribution = "gamma",
    prob_distribution_params = c(shape = 1, scale = 1)
  )

  # Get onset to hospital admission from {epiparameter} database
  onset_to_hosp <- epiparameter::epidist_db(
    disease = "COVID-19",
    epi_dist = "onset to hospitalisation",
    single_epidist = TRUE
  )

  # Get onset to death from {epiparameter} database
  onset_to_death <- epiparameter::epidist_db(
    disease = "COVID-19",
    epi_dist = "onset to death",
    single_epidist = TRUE
  )

  # Generate the full simulist
  set.seed(1)
  simulist_data <- simulist::sim_linelist(
    contact_distribution = contact_distribution,
    infectious_period = infectious_period,
    prob_infect = 0.205,
    onset_to_hosp = onset_to_hosp,
    onset_to_death = onset_to_death,
    onset_to_recovery = function(x) stats::rnorm(n = x, mean = 2.5, sd = 0.5),
    hosp_risk = 0.2,
    outbreak_start_date = as.Date("2019-12-01"),
    outbreak_size = c(1e3, 1e4),
    anonymise = TRUE
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

  # Remove surplus information
  simulist_data <- simulist_data |>
    dplyr::select(!dplyr::any_of(c("case_name", "date_first_contact", "date_last_contact", "ct_value")))

  # Store the simulated line list
  usethis::use_data(simulist_data, overwrite = TRUE)
}
