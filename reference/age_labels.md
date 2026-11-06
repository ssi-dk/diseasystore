# Provides sortable labels for age groups

Provides sortable labels for age groups

## Usage

``` r
age_labels(age_cuts)
```

## Arguments

- age_cuts:

  ([`numeric()`](https://rdrr.io/r/base/numeric.html))  
  The lower bound of the groups (0 is implicitly included).

## Value

A vector of labels with zero-padded numerics so they can be sorted
easily.

## Examples

``` r
  age_labels(c(5, 12, 20, 30))
#> [1] "00-04" "05-11" "12-19" "20-29" "30+"  
```
