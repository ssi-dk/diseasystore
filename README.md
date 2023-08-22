# diseasystore <img src="man/figures/logo.png" align="right" height="138" />
[![CRAN status](https://www.r-pkg.org/badges/version/diseasystore)](https://CRAN.R-project.org/package=diseasystore)
[![R-CMD-check](https://github.com/ssi-dk/diseasystore/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/ssi-dk/diseasystore/actions/workflows/R-CMD-check.yaml)
[![codecov](https://codecov.io/gh/ssi-dk/diseasystore/branch/main/graph/badge.svg?token=ZAUHJPQ28D)](https://codecov.io/gh/ssi-dk/diseasystore)

The `diseasystore` is a feature store implemented in R specifically designed for providing disease data for pandemic preparedness.
The package forms the data-backbone of the `diseasy` package.
## A modular feature store
The `diseasystore` is modularly built using R6 classes.
Different diseases are handled by individual feature stores that have access to all relevant disease data for the given disease.

## Handling of diverse data sources
Data for different diseases will typically be structured in different ways. The `diseasystore` currently implements the Google Health COVID-19 Open Repository with more one the way.

The `diseasystore` is designed to handled both individual level data (examples to come) or semi-aggregated (typically public) data.

If the data is at the individual level, the feature store is fully dynamic and  and can adapt to (virtually) any stratification that the user specifies.
If the data conversely is semi-aggregated, the data can only be stratified at the levels of the semi-aggregation (or at higher levels).

## Installation

You can install the development version of `diseasystore` from [GitHub](https://github.com/) with:

``` r
# install.packages("devtools")
devtools::install_github("ssi-dk/diseasystore")
```
