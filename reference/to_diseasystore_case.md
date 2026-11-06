# Transform case definition to PascalCase

Transform case definition to PascalCase

## Usage

``` r
to_diseasystore_case(label)
```

## Arguments

- label:

  (`character`)  
  A character string that controls which feature store to get data from.

## Value

The given label formatted to match a Diseasystore

## Examples

``` r
  to_diseasystore_case("Google COVID-19")  # DiseasystoreGoogleCovid19
#> [1] "DiseasystoreGoogleCovid19"
```
