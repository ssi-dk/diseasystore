# Changelog

## diseasystore 0.4.0

CRAN release: 2026-11-06

### Minor Improvements and Fixes

- Observables are now listed in `DiseasystoreEcdcRespiratoryViruses`
  ([\#227](https://github.com/ssi-dk/diseasystore/issues/227)).

- `%.%` made faster
  ([\#251](https://github.com/ssi-dk/diseasystore/issues/251)).

- Check for internet when connecting to online sources
  ([\#243](https://github.com/ssi-dk/diseasystore/issues/243)).

## diseasystore 0.3.3

CRAN release: 2026-03-20

### New Features

- Private method `DiseasystoreBase$determine_missing_ranges()` made
  public ([\#224](https://github.com/ssi-dk/diseasystore/issues/224)).
  Previously called `$determine_new_ranges()`.

### Minor Improvements and Fixes

- Compatibility with `duckdb v1.5.0`
  ([\#235](https://github.com/ssi-dk/diseasystore/issues/235)).

### Documentation

- Restrictions on automatic data-coupling is now better described
  ([\#227](https://github.com/ssi-dk/diseasystore/issues/227))

## diseasystore 0.3.2

CRAN release: 2025-10-17

### Minor Improvements and Fixes

- Compatibility with `stringr v.1.6.0`
  ([\#218](https://github.com/ssi-dk/diseasystore/issues/218),
  [@hadley](https://github.com/hadley)).

- Transfer of `Dates` to SQL Server now works again
  ([\#220](https://github.com/ssi-dk/diseasystore/issues/220)).

## diseasystore 0.3.1

CRAN release: 2025-02-28

### Breaking change

- `DiseasystoreBase$key_join_features()` no longer accept character
  stratifications – must use
  [`rlang::quos()`](https://rlang.r-lib.org/reference/defusing-advanced.html)
  ([\#203](https://github.com/ssi-dk/diseasystore/issues/203)).

### Minor Improvements and Fixes

- The `$observable_regex` field is added, which shows the regex that
  demarcates observables from stratifications
  ([\#204](https://github.com/ssi-dk/diseasystore/issues/204)).

- Improved clean up of temporary tables
  ([\#207](https://github.com/ssi-dk/diseasystore/issues/207)). Tables
  created internally in `diseasystore` are now labelled and removed when
  no longer needed.

- In `DiseasystoreBase$key_join_features()`, stratifications no longer
  need to do computation on other features
  ([\#203](https://github.com/ssi-dk/diseasystore/issues/203)).

## diseasystore 0.3.0

CRAN release: 2024-12-11

### Breaking change

- Additional arguments (`...`) are now passed to the
  `?FeatureHandler$compute()` and `?FeatureHandler$get()` functions in
  [`?FeatureHandler`](https://ssi-dk.github.io/diseasystore/reference/FeatureHandler.md)
  ([\#162](https://github.com/ssi-dk/diseasystore/issues/162)).
  Furthermore, a reference to the `diseasystore` is now passed to
  `?FeatureHandler$compute()` as `ds` to give the
  [`?FeatureHandler`](https://ssi-dk.github.io/diseasystore/reference/FeatureHandler.md)
  access to other features via `?DiseasystoreBase$get_feature()`.

### New Features

- New age helpers
  [`add_years()`](https://ssi-dk.github.io/diseasystore/reference/add_years.md)
  and
  [`age_on_date()`](https://ssi-dk.github.io/diseasystore/reference/age_on_date.md)
  has been added to help compute features for individual data
  ([\#125](https://github.com/ssi-dk/diseasystore/issues/125)).

- Added the fields `?DiseasystoreBase$available_observables` and
  `?DiseasystoreBase$available_features` for easier overview
  ([\#139](https://github.com/ssi-dk/diseasystore/issues/139)).

- The data availability period for each `diseasystore` is now exposed
  via the `?DiseasystoreBase$min_start_date` and
  `?DiseasystoreBase$max_end_date` fields
  ([\#138](https://github.com/ssi-dk/diseasystore/issues/138)).

- `diseasystores` can now have variable backend support
  ([\#175](https://github.com/ssi-dk/diseasystore/issues/175)). In case
  a backend is insufficient to support the computations, the backend can
  be blocked for the `diseasystore`.
  [`test_diseasystore()`](https://ssi-dk.github.io/diseasystore/reference/test_diseasystore.md)
  now have a `skip_backend` argument to skip tests for the disallowed
  backends.

- Added the
  [`?DiseasystoreSimulist`](https://ssi-dk.github.io/diseasystore/reference/DiseasystoreSimulist.md)
  which implements a synthetic, individual-level, data set generated
  with `simulist`.

### Minor Improvements and Fixes

- A bug was fixed where data was duplicated when features were not
  divided into separate tables
  ([\#192](https://github.com/ssi-dk/diseasystore/issues/192)).

- Two bugs were fixed in `?DiseasystoreBase$determine_new_ranges()`
  where existing tables were not detected:

  - On backends that use “catalog” to structure table (DuckDB and SQL
    Server) (also requires [SCDB](https://github.com/ssi-dk/SCDB) \>
    v0.4) ([\#158](https://github.com/ssi-dk/diseasystore/issues/158)).
  - Due to `slice_ts` not being correctly matched with existing data on
    some backends
    ([\#172](https://github.com/ssi-dk/diseasystore/issues/172)).

- Long stratification expression are now properly parsed in
  `?DiseasystoreBase$key_join_features()`
  ([\#161](https://github.com/ssi-dk/diseasystore/issues/161)).

- When creating your own `diseasystore` it is now easier to inherit from
  the base module
  ([\#189](https://github.com/ssi-dk/diseasystore/issues/189)).

- When creating your own `diseasystore` it is now easier to inherit from
  the base module
  ([\#189](https://github.com/ssi-dk/diseasystore/issues/189)).

- Warnings about existing stratifications now only appear if the
  requested stratification contains a named expression matching an
  existing stratification. Simply requesting the stratification from the
  feature store will no longer produce a warning
  ([\#192](https://github.com/ssi-dk/diseasystore/issues/192)).

- [`diseasyoption()`](https://ssi-dk.github.io/diseasystore/reference/diseasyoption.md)
  can now be called without the `option` argument to return all
  `diseasy/diseasystore` options
  ([\#159](https://github.com/ssi-dk/diseasystore/issues/159)).

  In addition, a new `namespace` argument can restrict the option
  look-up to a specific package (e.g. `diseasystore`).

### Testing

- [`test_diseasystore()`](https://ssi-dk.github.io/diseasystore/reference/test_diseasystore.md)
  now also checks the
  [`?FeatureHandler`](https://ssi-dk.github.io/diseasystore/reference/FeatureHandler.md)
  return data directly:
  - Checks that data is only within the study period
    ([\#154](https://github.com/ssi-dk/diseasystore/issues/154)).
  - Checks that `valid_from` and `valid_until` has class `Date`
    ([\#154](https://github.com/ssi-dk/diseasystore/issues/154)).
  - Checks that the `valid_from` and `valid_until` columns are
    chronologically ordered
    ([\#176](https://github.com/ssi-dk/diseasystore/issues/176)).

### Documentation

- An example has been added for building a `diseasystore` with
  individual level data
  ([\#162](https://github.com/ssi-dk/diseasystore/issues/162)). See
  [`vignette("extending-diseasystore-example")`](https://ssi-dk.github.io/diseasystore/articles/extending-diseasystore-example.md).

- Added benchmarking vignette
  [`vignette("benchmarks")`](https://ssi-dk.github.io/diseasystore/articles/benchmarks.md)
  ([\#144](https://github.com/ssi-dk/diseasystore/issues/144)).

## diseasystore 0.2.2

CRAN release: 2024-07-15

### Minor Improvements and Fixes

- The `%.%` operator is made more flexible to function as a drop-in
  replacement for `$`
  ([\#145](https://github.com/ssi-dk/diseasystore/issues/145)).

## diseasystore 0.2.1

CRAN release: 2024-04-22

- Support for [SCDB](https://github.com/ssi-dk/SCDB) v0.3 is removed.

## diseasystore 0.2.0

CRAN release: 2024-03-20

### New Features

- [`?DiseasystoreEcdcRespiratoryViruses`](https://ssi-dk.github.io/diseasystore/reference/DiseasystoreEcdcRespiratoryViruses.md):
  A feature store that uses the ECDC Respiratory viruses weekly
  repository
  ([\#124](https://github.com/ssi-dk/diseasystore/issues/124)).

- With the release of [SCDB](https://github.com/ssi-dk/SCDB) v0.4, we
  now support more database backends:

  - SQLite with attached schemas
    ([\#121](https://github.com/ssi-dk/diseasystore/issues/121)).

  - Microsoft SQL Server
    ([\#128](https://github.com/ssi-dk/diseasystore/issues/128)).

  - PostgreSQL
    ([\#128](https://github.com/ssi-dk/diseasystore/issues/128)).

  - DuckDB ([\#127](https://github.com/ssi-dk/diseasystore/issues/127)).

- [`test_diseasystore()`](https://ssi-dk.github.io/diseasystore/reference/test_diseasystore.md)
  is added to provide a standardised method for testing new
  diseasystores
  ([\#123](https://github.com/ssi-dk/diseasystore/issues/123)).

### Minor Improvements and Fixes

- [`diseasyoption()`](https://ssi-dk.github.io/diseasystore/reference/diseasyoption.md)
  now allows a default option to be set with the `.default` argument
  ([\#122](https://github.com/ssi-dk/diseasystore/issues/122)).

## diseasystore 0.1.1

CRAN release: 2024-01-18

### Fixes

- Improved the stability of internal functions.

- Adapted to release of [SCDB](https://github.com/ssi-dk/SCDB) v0.3.

### Documentation

- Improved documentation of functions.

### Testing

- Improved test stability when internet is unavailable.

- Reduced the data footprint during tests.

## diseasystore 0.1

CRAN release: 2023-12-20

### Features

- [`?DiseasystoreBase`](https://ssi-dk.github.io/diseasystore/reference/DiseasystoreBase.md):
  A base class for the diseasystores.

  - R6 class that defines the interface for the diseasystores.

- [`?DiseasystoreGoogleCovid19`](https://ssi-dk.github.io/diseasystore/reference/DiseasystoreGoogleCovid19.md):
  A feature store that uses the Google COVID-19 Open Data repository.

  - R6 class that builds on the base class to provide interface to the
    Google COVID-19 data.

- [`?FeatureHandler`](https://ssi-dk.github.io/diseasystore/reference/FeatureHandler.md):
  A simple class to handle individual feature computations.

  - Defines the interface for each individual feature in the
    diseasystores.

- Aggregators: A set of aggregators for the
  [`?FeatureHandler`](https://ssi-dk.github.io/diseasystore/reference/FeatureHandler.md)
  to use.

- [`drop_diseasystore()`](https://ssi-dk.github.io/diseasystore/reference/drop_diseasystore.md):
  A function to remove data from a feature store.

- Added a informative operator to access data `%.%`.

  - Gives error instead of `NULL` if element does not exist.

- [`age_labels()`](https://ssi-dk.github.io/diseasystore/reference/age_labels.md):
  A function to generate human-readable and sortable age groupings.

### Testing

- Most package functions are tested here.

### Documentation

- The functions are fully documented.

- Vignettes for the use of the package is included.

  - `vignette("quick-start")`
  - [`vignette("extending-diseasystore")`](https://ssi-dk.github.io/diseasystore/articles/extending-diseasystore.md)

- Vignette for the Google COVID-19 data is included.

  - [`vignette("diseasystore-google-covid-19")`](https://ssi-dk.github.io/diseasystore/articles/diseasystore-google-covid-19.md)
