# diseasystore (development version)

## New Features:

* New age helpers `add_years()` and `age_on_date()` has been added to help compute features for individual data (#125).

* Added the fields `$available_observables` and `$available_features` for easier overview (#139).

* The data availability period for each `diseasystore` is now exposed via the `$min_start_date` and `$max_end_date`
fields (#138).

## Minor Improvements and Fixes:

* Two bugs were fixed in `$determine_new_ranges()` where existing tables were not detected:
  * On back ends that use "catalog" to structure table (DuckDB and SQL Server) (fix also requires SCDB > v0.4) (#158).
  * Due to `slice_ts` not being correctly matched with existing data on some backends (#172).

* Long stratification expression are now properly parsed in `$key_join_features()` (#161).

## Testing:

* `test_diseasystore()` now also checks that the `FeatureHandler`s return data directly: (#154)
  * Checks that data is only within the study period.
  * Checks that `valid_from` and `valid_until` has class `Date`.


# diseasystore 0.2.2

## Minor Improvements and Fixes:

* The `%.%` operator is made more flexible to function as a drop-in replacement for `$` (#145).


# diseasystore 0.2.1

* Support for `{SCDB}` v0.3 is removed.


# diseasystore 0.2.0

## New Features:

* `DiseasystoreEcdcRespiratoryViruses`: A feature store that uses the ECDC Respiratory viruses weekly repository (#124).

* With the release of `{SCDB}` v0.4, we now support more database backends:

  * SQLite with attached schemas (#121).

  * Microsoft SQL Server (#128).

  * PostgreSQL (#128).

  * DuckDB (#127).

* `test_diseasystore()` is added to provide a standardised method for testing new diseasystores (#123).

## Minor Improvements and Fixes:

* `diseasyoption()` now allows a default option to be set with the `.default` argument (#122).


# diseasystore 0.1.1

## Fixes:

* Improved the stability of internal functions.

* Adapted to release of SCDB v0.3.

## Documentation:

* Improved documentation of functions.

## Testing:

* Improved test stability when internet is unavailable.

* Reduced the data footprint during tests.


# diseasystore 0.1

## Features:

* `DiseasystoreBase`: A base class for the diseasystores.
  * R6 class that defines the interface for the diseasystores.

* `DiseasystoreGoogleCovid19`: A feature store that uses the Google COVID-19 Open Data repository.
  * R6 class that builds on the base class to provide interface to the Google COVID-19 data.

* `FeatureHandler`: A simple class to handle individual feature computations.
  * Defines the interface for each individual feature in the diseasystores.

* Aggregators: A set of aggregators for the `FeatureHandler` to use.

* `drop_diseasystore()`: A function to remove data from a feature store.

* Added a informative operator to access data `%.%`.
  * Gives error instead of `NULL` if element does not exist.

* `age_labels()`: A function to generate human-readable and sortable age groupings.

## Testing:

* Most package functions are tested here.

## Documentation:

* The functions are fully documented.

* Vignettes for the use of the package is included.
  - `vignette("quick-start")`
  - `vignette("extending-diseasystore")`

* Vignette for the Google COVID-19 data is included.
  * `vignette("diseasystore-google-covid-19")`
