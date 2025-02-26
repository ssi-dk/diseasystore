# diseasystore (development version)

## Breaking change

* `DiseasystoreBase$key_join_features()` no longer accept character stratifications -- must use `rlang::quos()` (#203).

## Minor Improvements and Fixes

* The `$observable_regex` field is added, which shows the regex that demarcates observables from stratifications (#204).

* Improved clean up of temporary tables (#207).
  Tables created internally in `diseasystore` are now labelled and removed when no longer needed.

* In `DiseasystoreBase$key_join_features()`, stratifications no longer need to do computation on other features (#203).


# diseasystore 0.3.0

## Breaking change

* Additional arguments (`...`) are now passed to the `?FeatureHandler$compute()` and `?FeatureHandler$get()`
  functions in `?FeatureHandler` (#162).
  Furthermore, a reference to the `diseasystore` is now passed to `?FeatureHandler$compute()` as `ds` to give the
  `?FeatureHandler` access to other features via `?DiseasystoreBase$get_feature()`.

## New Features

* New age helpers `add_years()` and `age_on_date()` has been added to help compute features for individual data (#125).

* Added the fields `?DiseasystoreBase$available_observables` and `?DiseasystoreBase$available_features` for easier
  overview (#139).

* The data availability period for each `diseasystore` is now exposed via the `?DiseasystoreBase$min_start_date` and
  `?DiseasystoreBase$max_end_date` fields (#138).

* `diseasystores` can now have variable backend support (#175).
  In case a backend is insufficient to support the computations, the backend can be blocked for the `diseasystore`.
  `test_diseasystore()` now have a `skip_backend` argument to skip tests for the disallowed backends.

* Added the `?DiseasystoreSimulist` which implements a synthetic, individual-level, data set generated with `simulist`.

## Minor Improvements and Fixes

* A bug was fixed where data was duplicated when features were not divided into separate tables (#192).

* Two bugs were fixed in `?DiseasystoreBase$determine_new_ranges()` where existing tables were not detected:
  * On backends that use "catalog" to structure table (DuckDB and SQL Server) (also requires `{SCDB}` > v0.4) (#158).
  * Due to `slice_ts` not being correctly matched with existing data on some backends (#172).

* Long stratification expression are now properly parsed in `?DiseasystoreBase$key_join_features()` (#161).

* When creating your own `diseasystore` it is now easier to inherit from the base module (#189).

* When creating your own `diseasystore` it is now easier to inherit from the base module (#189).

* Warnings about existing stratifications now only appear if the requested stratification contains a named expression
  matching an existing stratification. Simply requesting the stratification from the feature store will no longer
  produce a warning (#192).

## Testing
* `test_diseasystore()` now also checks the `?FeatureHandler` return data directly:
  * Checks that data is only within the study period (#154).
  * Checks that `valid_from` and `valid_until` has class `Date` (#154).
  * Checks that the `valid_from` and `valid_until` columns are chronologically ordered (#176).

## Documentation

* An example has been added for building a `diseasystore` with individual level data (#162).
  See `vignette("extending-diseasystore-example")`.

* Added benchmarking vignette `vignette("benchmarks")` (#144).

## Minor Improvements and Fixes

* `diseasyoption()` can now be called without the `option` argument to return all `diseasy/diseasystore` options (#159).

  In addition, a new `namespace` argument can restrict the option look-up to a specific package (e.g. `diseasystore`).


# diseasystore 0.2.2

## Minor Improvements and Fixes:

* The `%.%` operator is made more flexible to function as a drop-in replacement for `$` (#145).


# diseasystore 0.2.1

* Support for `{SCDB}` v0.3 is removed.


# diseasystore 0.2.0

## New Features

* `?DiseasystoreEcdcRespiratoryViruses`: A feature store that uses the ECDC Respiratory viruses weekly repository (#124).

* With the release of `{SCDB}` v0.4, we now support more database backends:

  * SQLite with attached schemas (#121).

  * Microsoft SQL Server (#128).

  * PostgreSQL (#128).

  * DuckDB (#127).

* `test_diseasystore()` is added to provide a standardised method for testing new diseasystores (#123).

## Minor Improvements and Fixes

* `diseasyoption()` now allows a default option to be set with the `.default` argument (#122).


# diseasystore 0.1.1

## Fixes

* Improved the stability of internal functions.

* Adapted to release of `{SCDB}` v0.3.

## Documentation

* Improved documentation of functions.

## Testing

* Improved test stability when internet is unavailable.

* Reduced the data footprint during tests.


# diseasystore 0.1

## Features

* `?DiseasystoreBase`: A base class for the diseasystores.
  * R6 class that defines the interface for the diseasystores.

* `?DiseasystoreGoogleCovid19`: A feature store that uses the Google COVID-19 Open Data repository.
  * R6 class that builds on the base class to provide interface to the Google COVID-19 data.

* `?FeatureHandler`: A simple class to handle individual feature computations.
  * Defines the interface for each individual feature in the diseasystores.

* Aggregators: A set of aggregators for the `?FeatureHandler` to use.

* `drop_diseasystore()`: A function to remove data from a feature store.

* Added a informative operator to access data `%.%`.
  * Gives error instead of `NULL` if element does not exist.

* `age_labels()`: A function to generate human-readable and sortable age groupings.

## Testing

* Most package functions are tested here.

## Documentation

* The functions are fully documented.

* Vignettes for the use of the package is included.
  - `vignette("quick-start")`
  - `vignette("extending-diseasystore")`

* Vignette for the Google COVID-19 data is included.
  * `vignette("diseasystore-google-covid-19")`
