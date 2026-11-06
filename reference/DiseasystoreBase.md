# diseasystore base handler

This `DiseasystoreBase`
[R6](https://r6.r-lib.org/reference/R6Class.html) class forms the basis
of all feature stores. It defines the primary methods of each feature
stores as well as all of the public methods.

## Value

A new instance of the `DiseasystoreBase`
[R6](https://r6.r-lib.org/reference/R6Class.html) class.

## Active bindings

- `ds_map`:

  (`named list`(`character`))  
  A list that maps features known by the feature store to the
  corresponding feature handlers that compute the features. Read only.

- `available_features`:

  ([`character()`](https://rdrr.io/r/base/character.html))  
  A list of available features in the feature store. Read only.

- `available_observables`:

  ([`character()`](https://rdrr.io/r/base/character.html))  
  A list of available observables in the feature store. Read only.

- `available_stratifications`:

  ([`character()`](https://rdrr.io/r/base/character.html))  
  A list of available stratifications in the feature store. Read only.

- `observables_regex`:

  (`character(1)`)  
  A list of available stratifications in the feature store. Read only.

- `label`:

  (`character(1)`)  
  A human readable label of the feature store. Read only.

- `source_conn`:

  (`DBIConnection` or `file path`)  
  Used to specify where data is located. Read only. Can be
  `DBIConnection` or file path depending on the `diseasystore`.

- `target_conn`:

  (`DBIConnection`)  
  A database connection to store the computed features in. Read only.

- `target_schema`:

  (`character`)  
  The schema to place the feature store in. Read only. If the database
  backend does not support schema, the tables will be prefixed with
  `<target_schema>.`.

- `start_date`:

  (`Date`)  
  Study period start. Read only.

- `end_date`:

  (`Date`)  
  Study period end. Read only.

- `min_start_date`:

  (`Date`)  
  (Minimum)Study period start. Read only.

- `max_end_date`:

  (`Date`)  
  (Maximum)Study period end. Read only.

- `slice_ts`:

  (`Date` or `character`)  
  Date or timestamp (parsable by `as.POSIXct`) to slice the
  (time-versioned) data on. Read only.

## Methods

### Public methods

- [`DiseasystoreBase$new()`](#method-DiseasystoreBase-initialize)

- [`DiseasystoreBase$get_feature()`](#method-DiseasystoreBase-get_feature)

- [`DiseasystoreBase$key_join_features()`](#method-DiseasystoreBase-key_join_features)

- [`DiseasystoreBase$determine_missing_ranges()`](#method-DiseasystoreBase-determine_missing_ranges)

- [`DiseasystoreBase$clone()`](#method-DiseasystoreBase-clone)

------------------------------------------------------------------------

### `DiseasystoreBase$new()`

Creates a new instance of the `DiseasystoreBase`
[R6](https://r6.r-lib.org/reference/R6Class.html) class.

#### Usage

    DiseasystoreBase$new(
      start_date = NULL,
      end_date = NULL,
      slice_ts = NULL,
      source_conn = NULL,
      target_conn = NULL,
      target_schema = NULL,
      verbose = diseasyoption("verbose", self)
    )

#### Arguments

- `start_date`:

  (`Date`)  
  Study period start.

- `end_date`:

  (`Date`)  
  Study period end.

- `slice_ts`:

  (`Date` or `character`)  
  Date or timestamp (parsable by `as.POSIXct`) to slice the
  (time-versioned) data on.

- `source_conn`:

  (`DBIConnection` or `file path`)  
  Used to specify where data is located. Can be `DBIConnection` or file
  path depending on the `diseasystore`.

- `target_conn`:

  (`DBIConnection`)  
  A database connection to store the computed features in.

- `target_schema`:

  (`character`)  
  The schema to place the feature store in. If the database backend does
  not support schema, the tables will be prefixed with
  `<target_schema>.`.

- `verbose`:

  (`boolean`)  
  Boolean that controls enables debugging information.

#### Returns

A new instance of the `DiseasystoreBase`
[R6](https://r6.r-lib.org/reference/R6Class.html) class.

------------------------------------------------------------------------

### `DiseasystoreBase$get_feature()`

Computes, stores, and returns the requested feature for the study
period.

#### Usage

    DiseasystoreBase$get_feature(
      feature,
      start_date = self %.% start_date,
      end_date = self %.% end_date,
      slice_ts = self %.% slice_ts
    )

#### Arguments

- `feature`:

  (`character`)  
  The name of a feature defined in the feature store.

- `start_date`:

  (`Date`)  
  Study period start.

- `end_date`:

  (`Date`)  
  Study period end.

- `slice_ts`:

  (`Date` or `character`)  
  Date or timestamp (parsable by `as.POSIXct`) to slice the
  (time-versioned) data on.

#### Returns

A tbl_dbi with the requested feature for the study period.

------------------------------------------------------------------------

### `DiseasystoreBase$key_join_features()`

Joins various features from the feature store assuming a primary feature
(observable) that contains keys to witch the secondary features (defined
by `stratification`) are joined.

#### Usage

    DiseasystoreBase$key_join_features(
      observable,
      stratification = NULL,
      start_date = self %.% start_date,
      end_date = self %.% end_date
    )

#### Arguments

- `observable`:

  (`character`)  
  The observable to provide data or prediction for.

- `stratification`:

  (`list`(`quosures`) or `NULL`)  
  Use `rlang::quos(...)` to specify stratification. If given,
  expressions in stratification evaluated to give the stratification
  level.

- `start_date`:

  (`Date`)  
  Study period start.

- `end_date`:

  (`Date`)  
  Study period end.

#### Returns

A tbl_dbi with the requested joined features for the study period.

------------------------------------------------------------------------

### `DiseasystoreBase$determine_missing_ranges()`

Determine which dates are not already computed.

This method parses the `message` fields of the logs related to
`target_table` on the date given by `slice_ts`. These messages contains
information on the date-ranges that are already computed. Once parsed,
the method compares the computed date-ranges with the requested
date-range.

#### Usage

    DiseasystoreBase$determine_missing_ranges(
      target_table,
      start_date,
      end_date,
      slice_ts
    )

#### Arguments

- `target_table`:

  (`character` or `Id`)  
  The feature table to investigate.

- `start_date`:

  (`Date`)  
  Study period start.

- `end_date`:

  (`Date`)  
  Study period end.

- `slice_ts`:

  (`Date` or `character`)  
  Date or timestamp (parsable by `as.POSIXct`) to slice the
  (time-versioned) data on.

#### Returns

(`tibble`)  
A `data.frame` containing continuous date-ranges that require
computation to cover the requested period

------------------------------------------------------------------------

### `DiseasystoreBase$clone()`

The objects of this class are cloneable with this method.

#### Usage

    DiseasystoreBase$clone(deep = FALSE)

#### Arguments

- `deep`:

  Whether to make a deep clone.

## Examples

``` r
  # DiseasystoreBase is mostly used as the basis of other, more specific, classes
  # The DiseasystoreBase can be initialised individually if needed.

  ds <- DiseasystoreBase$new(source_conn = NULL,
                             target_conn = DBI::dbConnect(RSQLite::SQLite()))

  rm(ds)
```
