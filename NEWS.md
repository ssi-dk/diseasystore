# diseasystore 0.1.1

Fixes:
* DiseasystoreGoogleCovid19: fixed issue with start_date and end_date not being passed within key_join_filter()

# diseasystore 0.1.0

Features:
* DiseasystoreBase: A base class for the diseasystores
  * R6 class that defines the interface for the diseasystores
* DiseasystoreGoogleCovid19: A feature store that uses the Google COVID-19 Open Data repository
  * R6 class that builds on the base class to provide interface to the Google COVID-19 data
* FeatureHandler: A simple class to handle individual feature computations
  * Defines the interface for each individual feature in the diseasystores
* Aggregators: A set of aggregators for the FeatureHandlers to use
* Added a informative operator to access data `%.%`
  * Gives error instead of NULL if element does not exist
* A number of useful DB functions re-exported from internal package (to be released)

Testing:
* Most package functions are tested here
  * (Re-exported functions from internal package are tested elsewhere)

Documentation
* The functions are fully documented
* Vignette for the Google COVID-19 data is included
  * vignette("google_covid_19_data")
