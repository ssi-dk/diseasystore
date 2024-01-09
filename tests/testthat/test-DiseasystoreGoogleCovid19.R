# The files we need are stored remotely in Google's API
google_files <- c("by-age.csv", "demographics.csv", "index.csv", "weather.csv")

# Call the testing suite
test_diseasystore(
  DiseasystoreGoogleCovid19,
  get_test_conns,
  data_files = google_files,
  target_schema = target_schema_1
)
