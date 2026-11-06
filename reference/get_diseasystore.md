# Get the `diseasystore` for the case definition

Get the `diseasystore` for the case definition

## Usage

``` r
get_diseasystore(label)
```

## Arguments

- label:

  (`character`)  
  A character string that controls which feature store to get data from.

## Value

The diseasystore generator for the diseasystore matching the given label

## Examples

``` r
  ds <- get_diseasystore("Google COVID-19")  # Returns the DiseasystoreGoogleCovid19 generator
```
