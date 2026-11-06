# Test a given diseasy store

This function runs a battery of tests of the given diseasystore.

The supplied diseasystore must be a generator for the diseasystore, not
an instance of the diseasystore.

The tests assume that data has been made available locally to run the
majority of the tests. The location of the local data should be
configured in the options for "source_conn" of the given diseasystore
before calling test_diseasystore.

## Usage

``` r
test_diseasystore(
  diseasystore_generator = NULL,
  conn_generator = NULL,
  data_files = NULL,
  target_schema = "test_ds",
  test_start_date = NULL,
  skip_backends = NULL,
  ...
)
```

## Arguments

- diseasystore_generator:

  (`Diseasystore*`)  
  The diseasystore R6 class generator to test.

- conn_generator:

  (`function`)  
  Function that generates a `list`() of connections use as target_conn.
  Should take a `skip_backend` that does not open connections for the
  given backends.

- data_files:

  ([`character()`](https://rdrr.io/r/base/character.html))  
  List of files that should be available when testing.

- target_schema:

  (`character`)  
  The schema to place the feature store in. If the database backend does
  not support schema, the tables will be prefixed with
  `<target_schema>.`.

- test_start_date:

  (`Date`)  
  The earliest date to retrieve data from during tests.

- skip_backends:

  ([`character()`](https://rdrr.io/r/base/character.html))  
  List of connection types to skip tests for due to missing
  functionality.

- ...:

  Other parameters passed to the diseasystore generator.

## Value

`NULL` (called for side effects)

## Examples

``` r
# \donttest{
  # Generator for connections
  conns <- \(skip_backends) list(DBI::dbConnect(duckdb::duckdb()))

  # Call the testing suite
  test_diseasystore(
    diseasystore_generator = DiseasystoreSimulist,
    conn_generator = conns,
    target_schema = "test",
    test_start_date = as.Date("2020-02-20"),
    skip_backends = "SQLiteConnection"
  )
#> duckdb keeps downloaded extensions and secrets in a temporary directory:
#> ℹ /tmp/Rtmpywkfif/duckdb
#> This is removed when the R session ends.
#> • Extensions are re-downloaded each session.
#> • Secrets are lost.
#> ℹ Run duckdb(shared_home = TRUE) (or create ~/.duckdb) to keep them (suitable for most users).
#> ℹ Run duckdb(shared_home = FALSE) to accept the temporary directory (and silence this message).
#> ℹ See ?duckdb_storage for details and alternatives.
#> ── Skip: DiseasystoreSimulist initialises correctly ────────────────────────────
#> Reason: "SQLiteConnection" %in% skip_backends is TRUE
#> ── Skip: DiseasystoreSimulist can initialise with remote source_conn ───────────
#> Reason: "SQLiteConnection" %in% skip_backends is TRUE
#> ── Skip: DiseasystoreSimulist can initialise with default source_conn ──────────
#> Reason: "SQLiteConnection" %in% skip_backends is TRUE
#> duckdb keeps downloaded extensions and secrets in a temporary directory:
#> i /tmp/Rtmpywkfif/duckdb
#> This is removed when the R session ends.
#> * Extensions are re-downloaded each session.
#> * Secrets are lost.
#> i Run duckdb(shared_home = TRUE) (or create ~/.duckdb) to keep them (suitable for most users).
#> i Run duckdb(shared_home = FALSE) to accept the temporary directory (and silence this message).
#> i See ?duckdb_storage for details and alternatives.
#> ── Skip: Skipped test connection are also disallowed in DiseasystoreSimulist constructor  ──
#> Reason: empty test
#> duckdb keeps downloaded extensions and secrets in a temporary directory:
#> i /tmp/Rtmpywkfif/duckdb
#> This is removed when the R session ends.
#> * Extensions are re-downloaded each session.
#> * Secrets are lost.
#> i Run duckdb(shared_home = TRUE) (or create ~/.duckdb) to keep them (suitable for most users).
#> i Run duckdb(shared_home = FALSE) to accept the temporary directory (and silence this message).
#> i See ?duckdb_storage for details and alternatives.
#> Test passed with 2 successes 😀.
#> duckdb keeps downloaded extensions and secrets in a temporary directory:
#> i /tmp/Rtmpywkfif/duckdb
#> This is removed when the R session ends.
#> * Extensions are re-downloaded each session.
#> * Secrets are lost.
#> i Run duckdb(shared_home = TRUE) (or create ~/.duckdb) to keep them (suitable for most users).
#> i Run duckdb(shared_home = FALSE) to accept the temporary directory (and silence this message).
#> i See ?duckdb_storage for details and alternatives.
#> Note: method with signature 'DBIConnection#Id' chosen for function 'dbExistsTable',
#>  target signature 'duckdb_connection#Id'.
#>  "duckdb_connection#ANY" would also be valid
#> Test passed with 45 successes 🥳.
#> duckdb keeps downloaded extensions and secrets in a temporary directory:
#> i /tmp/Rtmpywkfif/duckdb
#> This is removed when the R session ends.
#> * Extensions are re-downloaded each session.
#> * Secrets are lost.
#> i Run duckdb(shared_home = TRUE) (or create ~/.duckdb) to keep them (suitable for most users).
#> i Run duckdb(shared_home = FALSE) to accept the temporary directory (and silence this message).
#> i See ?duckdb_storage for details and alternatives.
#> Test passed with 15 successes 🎊.
#> duckdb keeps downloaded extensions and secrets in a temporary directory:
#> ℹ /tmp/Rtmpywkfif/duckdb
#> This is removed when the R session ends.
#> • Extensions are re-downloaded each session.
#> • Secrets are lost.
#> ℹ Run duckdb(shared_home = TRUE) (or create ~/.duckdb) to keep them (suitable for most users).
#> ℹ Run duckdb(shared_home = FALSE) to accept the temporary directory (and silence this message).
#> ℹ See ?duckdb_storage for details and alternatives.
#> duckdb keeps downloaded extensions and secrets in a temporary directory:
#> i /tmp/Rtmpywkfif/duckdb
#> This is removed when the R session ends.
#> * Extensions are re-downloaded each session.
#> * Secrets are lost.
#> i Run duckdb(shared_home = TRUE) (or create ~/.duckdb) to keep them (suitable for most users).
#> i Run duckdb(shared_home = FALSE) to accept the temporary directory (and silence this message).
#> i See ?duckdb_storage for details and alternatives.
#> Test passed with 24 successes 🎊.
#> duckdb keeps downloaded extensions and secrets in a temporary directory:
#> i /tmp/Rtmpywkfif/duckdb
#> This is removed when the R session ends.
#> * Extensions are re-downloaded each session.
#> * Secrets are lost.
#> i Run duckdb(shared_home = TRUE) (or create ~/.duckdb) to keep them (suitable for most users).
#> i Run duckdb(shared_home = FALSE) to accept the temporary directory (and silence this message).
#> i See ?duckdb_storage for details and alternatives.
#> Test passed with 5 successes 🥇.
#> duckdb keeps downloaded extensions and secrets in a temporary directory:
#> i /tmp/Rtmpywkfif/duckdb
#> This is removed when the R session ends.
#> * Extensions are re-downloaded each session.
#> * Secrets are lost.
#> i Run duckdb(shared_home = TRUE) (or create ~/.duckdb) to keep them (suitable for most users).
#> i Run duckdb(shared_home = FALSE) to accept the temporary directory (and silence this message).
#> i See ?duckdb_storage for details and alternatives.
#> Test passed with 9 successes 🌈.
# }
```
