test_that("multiplication works", {

  # First we create a temporary directory for the Google COVID-19 data
  remote_conn <- getOption("diseasystore.DiseasystoreGoogleCovid19.remote_conn", default = stop("Option not set"))
  tmp_dir <- tempdir()
  options(diseasystore.DiseasystoreGoogleCovid19.source_conn = tmp_dir)
  options(diseasystore.DiseasystoreGoogleCovid19.target_conn = \() dbConnect(RSQLite::SQLite(), file.path(tmp_dir, "diseasystore_google_covid_19.sqlite")))


  # Then we download the first n rows of each data set of interest
  google_files <- c("by-age.csv", "demographics.csv", "index.csv", "weather.csv")
  purrr::walk(google_files, ~ {
    readr::read_csv(paste0(remote_conn, .), n_max = 1000, show_col_types = FALSE, progress = FALSE) |>
    readr::write_csv(file.path(tmp_dir, .))
  })

  fs <- DiseasystoreGoogleCovid19$new()

})
