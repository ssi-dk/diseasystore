# Call the testing suite
test_diseasystore(
  diseasystore_generator = DiseasystoreSimulist,
  conn_generator = get_test_conns,
  data_files = NULL,
  target_schema = target_schema_1,
  test_start_date = as.Date("2019-12-01")
)
