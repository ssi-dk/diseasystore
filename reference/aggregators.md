# title Feature aggregators

When designing a `diseasystore` (see
[`vignette("extending-diseasystore")`](https://ssi-dk.github.io/diseasystore/articles/extending-diseasystore.md)),
each feature must specify how that data should be summarised when joined
with other data.

The automatic coupling and aggregation is essentially a three part
process: First, data for the observable is joined with data required to
form the stratifications. Next, the combined data is grouped at the
level of stratification requested. Finally, the data is summarised in
each group to form a single value for the group.

Below, we provide a set of simple function that perform this final
summarisation step.

For semi-aggregated data, a typical key_join aggregator is the
`key_join_sum()` which just adds the observations across the group

In some cases, such as if the observable is the "maximum temperature"
the relevant aggregator would be the `key_join_max()` aggregator which
takes the max across the group.

When working with individual level data, the observables often consists
of a record per individual and to summarise the data we instead need the
`key_join_count()` aggregator to get the size of the group.

## Usage

``` r
key_join_sum(.data, feature)

key_join_max(.data, feature)

key_join_min(.data, feature)

key_join_count(.data, feature)
```

## Arguments

- .data:

  (`any`)  
  The data object to perform the operation on.

- feature:

  (`character`)  
  Name of the feature to perform the aggregation over

## Value

A dplyr::summarise to aggregate the features together using the given
function (sum/max/min/count)

## Examples

``` r
  # Primarily used within the framework but can be used individually:

  data <- dplyr::mutate(mtcars, key_name = rownames(mtcars), .before = dplyr::everything())

  key_join_sum(data, "mpg")    # sum(mtcars$mpg)
#>   value
#> 1 642.9
  key_join_max(data, "mpg")    # max(mtcars$mpg)
#>   value
#> 1  33.9
  key_join_min(data, "mpg")    # min(mtcars$mpg)
#>   value
#> 1  10.4
  key_join_count(data, "mpg")  # nrow(mtcars)
#>   value
#> 1    32
```
