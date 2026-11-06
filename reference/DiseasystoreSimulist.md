# feature store handler of synthetic `simulist` features

This `DiseasystoreSimulist`
[R6](https://r6.r-lib.org/reference/R6Class.html) brings support for
individual level data.

## Value

A new instance of the `DiseasystoreSimulist`
[R6](https://r6.r-lib.org/reference/R6Class.html) class.

## Super class

[`DiseasystoreBase`](https://ssi-dk.github.io/diseasystore/reference/DiseasystoreBase.md)
-\> `DiseasystoreSimulist`

## Methods

### Public methods

- [`DiseasystoreSimulist$new()`](#method-DiseasystoreSimulist-initialize)

- [`DiseasystoreSimulist$clone()`](#method-DiseasystoreSimulist-clone)

Inherited methods

- [`DiseasystoreBase$determine_missing_ranges()`](https://ssi-dk.github.io/diseasystore/reference/DiseasystoreBase.html#method-determine_missing_ranges)
- [`DiseasystoreBase$get_feature()`](https://ssi-dk.github.io/diseasystore/reference/DiseasystoreBase.html#method-get_feature)
- [`DiseasystoreBase$key_join_features()`](https://ssi-dk.github.io/diseasystore/reference/DiseasystoreBase.html#method-key_join_features)

------------------------------------------------------------------------

### `DiseasystoreSimulist$new()`

Creates a new instance of the `DiseasystoreSimulist`
[R6](https://r6.r-lib.org/reference/R6Class.html) class.

#### Usage

    DiseasystoreSimulist$new(...)

#### Arguments

- `...`:

  Arguments passed to the
  [`?DiseasystoreBase`](https://ssi-dk.github.io/diseasystore/reference/DiseasystoreBase.md)
  constructor.

#### Returns

A new instance of the `DiseasystoreSimulist`
[R6](https://r6.r-lib.org/reference/R6Class.html) class.

------------------------------------------------------------------------

### `DiseasystoreSimulist$clone()`

The objects of this class are cloneable with this method.

#### Usage

    DiseasystoreSimulist$clone(deep = FALSE)

#### Arguments

- `deep`:

  Whether to make a deep clone.

## Examples

``` r
  ds <- DiseasystoreSimulist$new(
    source_conn = ".",
    target_conn = DBI::dbConnect(duckdb::duckdb())
  )
#> duckdb keeps downloaded extensions and secrets in a temporary directory:
#> ℹ /tmp/Rtmpywkfif/duckdb
#> This is removed when the R session ends.
#> • Extensions are re-downloaded each session.
#> • Secrets are lost.
#> ℹ Run duckdb(shared_home = TRUE) (or create ~/.duckdb) to keep them (suitable for most users).
#> ℹ Run duckdb(shared_home = FALSE) to accept the temporary directory (and silence this message).
#> ℹ See ?duckdb_storage for details and alternatives.


  rm(ds)
```
