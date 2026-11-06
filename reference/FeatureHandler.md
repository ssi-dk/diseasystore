# FeatureHandler

This `FeatureHandler` [R6](https://r6.r-lib.org/reference/R6Class.html)
handles individual features for the feature stores. They define the
three methods associated with features (`compute`, `get` and
`key_join`).

## Value

A new instance of the `FeatureHandler`
[R6](https://r6.r-lib.org/reference/R6Class.html) class.

## Active bindings

- `compute`:

  (`function`)  
  A function of the form "function(start_date, end_date, slice_ts,
  source_conn, ds (optional), ...)". This function should compute the
  feature from the source connection.

- `get`:

  (`function`)  
  A function of the form "function(target_table, slice_ts,
  target_conn)". This function should retrieve the computed feature from
  the target connection.

- `key_join`:

  (`function`)  
  One of the aggregators from
  [aggregators](https://ssi-dk.github.io/diseasystore/reference/aggregators.md).

## Methods

### Public methods

- [`FeatureHandler$new()`](#method-FeatureHandler-initialize)

- [`FeatureHandler$clone()`](#method-FeatureHandler-clone)

------------------------------------------------------------------------

### `FeatureHandler$new()`

Creates a new instance of the `FeatureHandler`
[R6](https://r6.r-lib.org/reference/R6Class.html) class.

#### Usage

    FeatureHandler$new(compute = NULL, get = NULL, key_join = NULL)

#### Arguments

- `compute`:

  (`function`)  
  A function of the form "function(start_date, end_date, slice_ts,
  source_conn, ds (optional), ...)".

  This function should return a `data.frame` with the computed feature
  (computed from the source connection). The `data.frame` should contain
  the following columns:

  - key\_\*: One (or more) columns containing keys to link this feature
    with other features

  - \*: One (or more) columns containing the features that are computed

  - valid_from, valid_until: A set of columns containing the time period
    for which this feature information is valid.  

- `get`:

  (`function`)  
  (Optional). A function of the form "function(target_table, slice_ts,
  target_conn, ...)". This function should retrieve the computed feature
  from the target connection.  

- `key_join`:

  (`function`)  
  A function like one of the aggregators from
  [`aggregators()`](https://ssi-dk.github.io/diseasystore/reference/aggregators.md).

  The function should return an expression on the form:
  dplyr::summarise(.data, dplyr::across(.cols =
  tidyselect::all_of(feature), .fns = list(n = ~ aggregation function),
  .names = "{.fn}"), .groups = "drop")

#### Returns

A new instance of the `FeatureHandler`
[R6](https://r6.r-lib.org/reference/R6Class.html) class.

------------------------------------------------------------------------

### `FeatureHandler$clone()`

The objects of this class are cloneable with this method.

#### Usage

    FeatureHandler$clone(deep = FALSE)

#### Arguments

- `deep`:

  Whether to make a deep clone.

## Examples

``` r
  # The FeatureHandler is typically configured as part of making a new Diseasystore.
  # Most often, we need only specify `compute` and `key_join` to get a functioning FeatureHandler

  # In this example we use mtcars as the basis for our features
  conn <- SCDB::get_connection(drv = RSQLite::SQLite())

  # We use mtcars as our basis. First we add the rownames as an actual column
  data <- dplyr::mutate(mtcars, key_name = rownames(mtcars), .before = dplyr::everything())

  # Then we add some imaginary times where these cars were produced
  data <- dplyr::mutate(data,
                        production_start = as.Date(Sys.Date()) + floor(runif(nrow(mtcars)) * 100),
                        production_end   = production_start + floor(runif(nrow(mtcars)) * 365))

  dplyr::copy_to(conn, data, "mtcars")

  # In this example, the feature we want is the "maximum miles per gallon"
  # The feature in question in the mtcars data set is then "mpg" and when we need to reduce
  # our data set, we want to use the "max()" function.

  # We first write a compute function for the mpg in our modified mtcars data set
  # Our goal is to get the mpg of all cars that were in production at the between start/end_date
  compute_mpg <- function(start_date, end_date, slice_ts, source_conn) {
    out <- SCDB::get_table(source_conn, "mtcars", slice_ts = slice_ts) |>
      dplyr::filter({{ start_date }} <= .data$production_end,
                    .data$production_start <= {{ end_date }}) |>
      dplyr::transmute("key_name", "mpg",
                       "valid_from" = "production_start",
                       "valid_until" = "production_end")

    return(out)
  }

  # We can now combine into our FeatureHandler
  fh_max_mpg <- FeatureHandler$new(compute = compute_mpg, key_join = key_join_max)

  DBI::dbDisconnect(conn)
```
