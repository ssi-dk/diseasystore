# Check for the existence of a `diseasystore` for the case definition

Check for the existence of a `diseasystore` for the case definition

## Usage

``` r
diseasystore_exists(label)
```

## Arguments

- label:

  (`character`)  
  A character string that controls which feature store to get data from.

## Value

TRUE if the given diseasystore can be matched to a diseasystore on the
search path. FALSE otherwise.

## Examples

``` r
  diseasystore_exists("Google COVID-19")  # TRUE
#> [1] TRUE
  diseasystore_exists("Non existent diseasystore")  # FALSE
#> [1] FALSE
```
