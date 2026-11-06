# feature store handler of EU-ECDC Respiratory viruses features

This `DiseasystoreEcdcRespiratoryViruses`
[R6](https://r6.r-lib.org/reference/R6Class.html) brings support for
using the EU-ECDC Respiratory viruses weekly data repository. See the
vignette("diseasystore-ecdc-respiratory-viruses") for details on how to
configure the feature store.

## Value

A new instance of the `DiseasystoreEcdcRespiratoryViruses`
[R6](https://r6.r-lib.org/reference/R6Class.html) class.

## Super class

[`DiseasystoreBase`](https://ssi-dk.github.io/diseasystore/reference/DiseasystoreBase.md)
-\> `DiseasystoreEcdcRespiratoryViruses`

## Methods

### Public methods

- [`DiseasystoreEcdcRespiratoryViruses$new()`](#method-DiseasystoreEcdcRespiratoryViruses-initialize)

- [`DiseasystoreEcdcRespiratoryViruses$clone()`](#method-DiseasystoreEcdcRespiratoryViruses-clone)

Inherited methods

- [`DiseasystoreBase$determine_missing_ranges()`](https://ssi-dk.github.io/diseasystore/reference/DiseasystoreBase.html#method-determine_missing_ranges)
- [`DiseasystoreBase$get_feature()`](https://ssi-dk.github.io/diseasystore/reference/DiseasystoreBase.html#method-get_feature)
- [`DiseasystoreBase$key_join_features()`](https://ssi-dk.github.io/diseasystore/reference/DiseasystoreBase.html#method-key_join_features)

------------------------------------------------------------------------

### `DiseasystoreEcdcRespiratoryViruses$new()`

Creates a new instance of the `DiseasystoreEcdcRespiratoryViruses`
[R6](https://r6.r-lib.org/reference/R6Class.html) class.

#### Usage

    DiseasystoreEcdcRespiratoryViruses$new(...)

#### Arguments

- `...`:

  Arguments passed to the
  [`?DiseasystoreBase`](https://ssi-dk.github.io/diseasystore/reference/DiseasystoreBase.md)
  constructor.

#### Returns

A new instance of the `DiseasystoreEcdcRespiratoryViruses`
[R6](https://r6.r-lib.org/reference/R6Class.html) class.

------------------------------------------------------------------------

### `DiseasystoreEcdcRespiratoryViruses$clone()`

The objects of this class are cloneable with this method.

#### Usage

    DiseasystoreEcdcRespiratoryViruses$clone(deep = FALSE)

#### Arguments

- `deep`:

  Whether to make a deep clone.

## Examples

``` r
  ds <- DiseasystoreEcdcRespiratoryViruses$new(
    source_conn = ".",
    target_conn = DBI::dbConnect(RSQLite::SQLite())
  )


  rm(ds)
```
