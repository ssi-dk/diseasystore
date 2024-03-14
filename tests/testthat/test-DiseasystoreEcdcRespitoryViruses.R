# The files we need are stored remotely on GitHub
ecdc_files <- "data/snapshots/2023-11-24_ILIARIRates.csv"

withr::local_options("diseasystore.DiseasystoreEcdcRespiratoryViruses.pull" = FALSE)

# Call the testing suite
test_diseasystore(
  diseasystore_generator = DiseasystoreEcdcRespiratoryViruses,
  conn_generator = get_test_conns,
  data_files = ecdc_files,
  target_schema = target_schema_1,
  test_start_date = as.Date("2022-06-20"),
  slice_ts = "2023-11-24"
)
