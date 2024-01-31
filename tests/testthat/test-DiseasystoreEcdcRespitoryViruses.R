# The files we need are stored remotely on GitHub
ecdc_files <- "data/snapshots/2023-11-24_ILIARIRates.csv"

withr::local_options("diseasystore.DiseasystoreEcdcRespitoryViruses.pull" = FALSE)

# Call the testing suite
test_diseasystore(
  DiseasystoreEcdcRespitoryViruses,
  get_test_conns,
  data_files = ecdc_files,
  target_schema = target_schema_1
)
