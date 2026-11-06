# feature store handler of Google Health COVID-19 Open Data features

This `DiseasystoreGoogleCovid19`
[R6](https://r6.r-lib.org/reference/R6Class.html) brings support for
using the Google Health COVID-19 Open Data repository. See the
vignette("diseasystore-google-covid-19") for details on how to configure
the feature store.

## Value

A new instance of the `DiseasystoreGoogleCovid19`
[R6](https://r6.r-lib.org/reference/R6Class.html) class.

## Super class

[`DiseasystoreBase`](https://ssi-dk.github.io/diseasystore/reference/DiseasystoreBase.md)
-\> `DiseasystoreGoogleCovid19`

## Methods

### Public methods

- [`DiseasystoreGoogleCovid19$clone()`](#method-DiseasystoreGoogleCovid19-clone)

Inherited methods

- [`DiseasystoreBase$determine_missing_ranges()`](https://ssi-dk.github.io/diseasystore/reference/DiseasystoreBase.html#method-determine_missing_ranges)
- [`DiseasystoreBase$get_feature()`](https://ssi-dk.github.io/diseasystore/reference/DiseasystoreBase.html#method-get_feature)
- [`DiseasystoreBase$initialize()`](https://ssi-dk.github.io/diseasystore/reference/DiseasystoreBase.html#method-initialize)
- [`DiseasystoreBase$key_join_features()`](https://ssi-dk.github.io/diseasystore/reference/DiseasystoreBase.html#method-key_join_features)

------------------------------------------------------------------------

### `DiseasystoreGoogleCovid19$clone()`

The objects of this class are cloneable with this method.

#### Usage

    DiseasystoreGoogleCovid19$clone(deep = FALSE)

#### Arguments

- `deep`:

  Whether to make a deep clone.

## Examples

``` r
  ds <- DiseasystoreGoogleCovid19$new(
    source_conn = ".",
    target_conn = DBI::dbConnect(RSQLite::SQLite())
  )


  rm(ds)
```
