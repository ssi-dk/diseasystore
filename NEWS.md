# diseasystore 0.1.1

Features:
* drop_diseasystore: A function to remove data from a feature store

* DiseasystoreBase:
  * Added checks to target_conn and source_conn when initializing
  * Improved and added active bindings
  * Added error message to truncate_interlace if no common keys found
  * Tagged features named "\*temperature" as observables

* DiseasystoreGoogleCovid19:
  * Renamed "\*_temp" observables to "\*_temperature"

Fixes:
* DiseasystoreBase:
  * All options now properly exported

* DiseasystoreGoogleCovid19:
  * Fixed issue with start_date and end_date not being passed within key_join_filter()
  * Fixed conversion of boolean to numeric for "n_ventilator"
  * All options now properly exported

* key_join_features:
  * Changed to dplyr::\*\_joins instead of mg_\*_joins

Documentation:
* Improved documentation templates


# diseasystore 0.1

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
