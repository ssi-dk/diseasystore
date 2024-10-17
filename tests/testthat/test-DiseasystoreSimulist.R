# Call the testing suite
test_diseasystore(
  diseasystore_generator = DiseasystoreSimulist,
  conn_generator = get_test_conns,
  data_files = NULL,
  target_schema = target_schema_1,
  test_start_date = as.Date("2020-02-20"),
  # SQLite does not have sufficient date support and dbplyr has inconsistent conversion of dates for SQLite.
  # Accounting for all of these issues defeats the purpose of `DiseasystoreSimulist` being a example of how to build
  # a diseasystore
  skip_backends = c("SQLiteConnection", "Microsoft SQL Server")
)
