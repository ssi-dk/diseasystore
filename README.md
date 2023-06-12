# diseasystore <img src="man/figures/logo.png" align="right" height="138" />
The "diseasystore" is a feature store implemented in R specifically designed for providing disease data for pandemic preparedness.

# A modular feature store
The "diseasystore" is modularly built using R6 classes. 
Different diseases are specified by a "case_definition" which implements a feature store that has access to all relevant disease data for the given case definition.
Generic disease data for one case definition is also available for other case definitions reducing the overhead of the data storage.

# Handling of diverse data sources
Data for different diseases will typically be structured in different ways. The "diseasystore" includes examples for how to implement your own disease data set.

The "diseasystore" is designed to handled both individual level data (preferred) or semi-aggregated data. 
If the data is at the individual level, the feature store is fully dynamics and can adapt to (virtually) any stratification that the user specifies.
If the data conversely is semi-aggregated, the data can only be stratified at the levels of the semi-aggregation (or at higher levels).
