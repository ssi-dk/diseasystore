# Package index

## Diseasystores

Feature stores that provide disease outbreak data.

- [`DiseasystoreBase`](https://ssi-dk.github.io/diseasystore/reference/DiseasystoreBase.md)
  : diseasystore base handler

- [`DiseasystoreEcdcRespiratoryViruses`](https://ssi-dk.github.io/diseasystore/reference/DiseasystoreEcdcRespiratoryViruses.md)
  : feature store handler of EU-ECDC Respiratory viruses features

- [`DiseasystoreGoogleCovid19`](https://ssi-dk.github.io/diseasystore/reference/DiseasystoreGoogleCovid19.md)
  : feature store handler of Google Health COVID-19 Open Data features

- [`DiseasystoreSimulist`](https://ssi-dk.github.io/diseasystore/reference/DiseasystoreSimulist.md)
  :

  feature store handler of synthetic `simulist` features

## Helper functions

Tools to interact with Diseasystores.

- [`age_labels()`](https://ssi-dk.github.io/diseasystore/reference/age_labels.md)
  : Provides sortable labels for age groups

- [`age_on_date()`](https://ssi-dk.github.io/diseasystore/reference/age_on_date.md)
  : Compute the age (in years) on a given date

- [`add_years()`](https://ssi-dk.github.io/diseasystore/reference/add_years.md)
  : Backend-dependent time interval (in years)

- [`available_diseasystores()`](https://ssi-dk.github.io/diseasystore/reference/available_diseasystores.md)
  : Detect available diseasystores

- [`diseasyoption()`](https://ssi-dk.github.io/diseasystore/reference/diseasyoption.md)
  : Helper function to get options related to diseasy

- [`drop_diseasystore()`](https://ssi-dk.github.io/diseasystore/reference/drop_diseasystore.md)
  : Drop feature stores from DB

- [`get_diseasystore()`](https://ssi-dk.github.io/diseasystore/reference/get_diseasystore.md)
  :

  Get the `diseasystore` for the case definition

- [`diseasystore_exists()`](https://ssi-dk.github.io/diseasystore/reference/diseasystore_exists.md)
  :

  Check for the existence of a `diseasystore` for the case definition

- [`to_diseasystore_case()`](https://ssi-dk.github.io/diseasystore/reference/to_diseasystore_case.md)
  : Transform case definition to PascalCase

## Developer functions

Functions to create and manage Diseasystores.

- [`DiseasystoreBase`](https://ssi-dk.github.io/diseasystore/reference/DiseasystoreBase.md)
  : diseasystore base handler
- [`FeatureHandler`](https://ssi-dk.github.io/diseasystore/reference/FeatureHandler.md)
  : FeatureHandler
- [`key_join_sum()`](https://ssi-dk.github.io/diseasystore/reference/aggregators.md)
  [`key_join_max()`](https://ssi-dk.github.io/diseasystore/reference/aggregators.md)
  [`key_join_min()`](https://ssi-dk.github.io/diseasystore/reference/aggregators.md)
  [`key_join_count()`](https://ssi-dk.github.io/diseasystore/reference/aggregators.md)
  : title Feature aggregators
- [`test_diseasystore()`](https://ssi-dk.github.io/diseasystore/reference/test_diseasystore.md)
  : Test a given diseasy store
- [`source_conn_path()`](https://ssi-dk.github.io/diseasystore/reference/source_conn_helpers.md)
  [`source_conn_github()`](https://ssi-dk.github.io/diseasystore/reference/source_conn_helpers.md)
  : File path helper for different source_conn
- [`` `%.%` ``](https://ssi-dk.github.io/diseasystore/reference/grapes-.-grapes.md)
  : Existence aware pick operator
