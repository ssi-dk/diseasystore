# diseasystore 0.0.0.9005

Features:
* New mg function reexports

Fixes:
* Fixed issues with key_join_features
* Fixed issues with key_join_filter
* Removed na.rm warnings

Testing:
* Added tests for key_join_features

# diseasystore 0.0.0.9004

Features
* Added a destructor to DiseasystoreBase
* Documented and exported %.% operator

Documentation
* Updated README
* Grouped all mg function into single doc entry
* Improvied vignette google_covid_19_data
* Removed TODO's in DiseasystoreCovid19 and FeatureHandler

# diseasystore 0.0.0.9003

Features
* New mg reexports (functions are now fully documented)
* interlace renamed to truncate_interlace

Fixes
* diseasystore_case_definition now works with snake_case

Testing
* Resolved the test failure for code coverage
* Added tests for the aggregators
* Added tests for truncate_interlace
* Added tests for feature_store_helpers

Documentation
* Added documentation for truncate_interlace


# diseasystore 0.0.0.9002

Features
* Exported functions from mg to diseasystore
* Added the FeatureHandler class to handle individual feature computations
* Added a set of aggregators for the FeatureHandler
* Added the DiseasystoreCovid19 feature store
* Updates to DiseasystoreBase to work with subclasses
* Added a informative operator to access data `%.%`
* Added active binding to DiseasystoreBase that shows available features

Testing
* Added tests for DiseasystoreGoogleCovid19
  * Test that features are generated as expected by FeatureHandler
  * Tests for different ranges of dates

# diseasystore 0.0.0.9001

Features
* Added a base class for the diseasystores

Fixes
* Added authors to DESCRIPTION

Chores
* Resolved lint issues

# diseasystore 0.0.0.9000

* Added a `NEWS.md` file to track changes to the package.
