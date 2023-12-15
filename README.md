
<!-- README.md is generated from README.Rmd. Please edit that file. -->

# diseasystore <a href="https://ssi-dk.github.io/diseasystore/"><img src="man/figures/logo.png" alt="SCDB website" align="right" height="138"/></a>

<!-- badges: start -->

[![CRAN
status](https://www.r-pkg.org/badges/version/diseasystore)](https://CRAN.R-project.org/package=diseasystore)
[![R-CMD-check](https://github.com/ssi-dk/diseasystore/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/ssi-dk/diseasystore/actions/workflows/R-CMD-check.yaml)
[![codecov](https://codecov.io/gh/ssi-dk/diseasystore/branch/main/graph/badge.svg)](https://app.codecov.io/gh/ssi-dk/diseasystore)

<!-- badges: end -->

## Overview

The `diseasystore` package provides feature stores implemented in R
specifically designed for serve disease data for epidemic preparedness.

What makes a `diseasystore` special, is that features can be
automatically coupled and stratified within the `diseasystore` package.
Consult the Quick start vignette to see it in action
(`vignette("quick-start", package = "diseasystore")`).

The package forms the data-backbone of the `{diseasy}` package.

## Handling of diverse data sources

Different data sources are handled by individual `diseasystores` which
each facilitate access to the relevant disease data for the given data
source.

Data for different diseases will typically be structured in different
ways. The `diseasystore` package currently implements the Google Health
COVID-19 Open Repository with more one `diseasystores` the way.

The `diseasystore` package is designed to handled both individual-level
data (examples to come) and semi-aggregated (typically publicly
available) data.

If the data is at the individual-level, the feature store is fully
dynamic and can adapt to (virtually) any stratification that the user
specifies. If the data conversely is semi-aggregated, the data can only
be stratified at the levels of the semi-aggregation (or at higher
levels).

## Installation

``` r
# Install diseasystore from CRAN:
install.packages("diseasystore")

# Alternatively, install the development version from github:
# install.packages("devtools")
devtools::install_github("ssi-dk/diseasystore")
```
