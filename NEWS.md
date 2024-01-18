# diseasystore (development version)

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
