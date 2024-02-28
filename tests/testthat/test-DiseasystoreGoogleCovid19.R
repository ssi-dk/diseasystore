# The files we need are stored remotely in Google's API
google_files <- c("by-age.csv", "demographics.csv", "index.csv", "weather.csv")

# Call the testing suite
test_diseasystore(
  diseasystore_generator = DiseasystoreGoogleCovid19,
  conn_generator = get_test_conns,
  data_files = google_files,
  target_schema = target_schema_1,
  test_start_date = as.Date("2020-03-01")
)
