if (rlang::is_installed("epiparameter") && rlang::is_installed("simulist") && rlang::is_installed("usethis")) {

  # Configure `simulist` using their quick-start configuration.

  # Create COVID-19 contact distribution
  contact_distribution <- epiparameter::epidist(
    disease = "COVID-19",
    epi_dist = "contact distribution",
    prob_distribution = "pois",
    prob_distribution_params = c(mean = 2)
  )

  # Create COVID-19 contact interval
  contact_interval <- epiparameter::epidist(
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
    contact_interval = contact_interval,
    prob_infect = 0.5,
    onset_to_hosp = onset_to_hosp,
    onset_to_death = onset_to_death,
    hosp_risk = 0.2,
    outbreak_start_date = as.Date("2019-12-01"),
    min_outbreak_size = 1000,
    add_names = FALSE,
    add_ct = FALSE
  )

  # Convert to tibble
  simulist_data <- tibble::as_tibble(simulist_data)

  # Convert dates from Date to Date (sic!)
  # ... Somehow R has stored extra information in the dates that makes them unique despite being the same date
  simulist_data <- simulist_data |>
    dplyr::mutate(dplyr::across(tidyselect::starts_with("date_"), ~ as.Date(as.character(.))))

  # Store the simulated linelist
  usethis::use_data(simulist_data, overwrite = TRUE)
}
