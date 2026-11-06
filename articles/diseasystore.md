# diseasystore: quick start guide

``` r

library(diseasystore)
```

## Available diseasystores

To see the available `diseasystores` on your system, you can use the
[`available_diseasystores()`](https://ssi-dk.github.io/diseasystore/reference/available_diseasystores.md)
function.

``` r

available_diseasystores()
#> [1] "DiseasystoreEcdcRespiratoryViruses" "DiseasystoreGoogleCovid19"         
#> [3] "DiseasystoreSimulist"
```

This function looks for `diseasystores` on the current search path. By
default, this will show the `diseasystores` bundled with the base
package. If you have
[extended](https://ssi-dk.github.io/diseasystore/articles/extending-diseasystore.md)
`diseasystore` with either your own `diseasystores` or from an external
package, then attaching the package to your search path will allow it to
show up as available.

Note: `diseasystores` are found if they are defined within packages
named `diseasystore*` and are of the class
[`?DiseasystoreBase`](https://ssi-dk.github.io/diseasystore/reference/DiseasystoreBase.md).

Each of these `diseasystores` may have their own vignette that further
details their content, use and/or tips and tricks. This is for example
the case with
[`?DiseasystoreGoogleCovid19`](https://ssi-dk.github.io/diseasystore/reference/DiseasystoreGoogleCovid19.md).

## Using a diseasystore

To use a `diseasystore` we need to first do some configuration. The
`diseasystores` are designed to work with data bases to store the
computed features in. Each `diseasystore` may require individual
configuration as listed in its documentation or accompanying vignette.

For this Quick start, we will configure a
[`?DiseasystoreGoogleCovid19`](https://ssi-dk.github.io/diseasystore/reference/DiseasystoreGoogleCovid19.md)
to use a local [duckdb](https://r.duckdb.org/) data base Ideally, we
want to use a faster, more capable, data base to store the features in.
The `diseasystores` uses [SCDB](https://github.com/ssi-dk/SCDB) in the
back end and can use any data base back end supported by
[SCDB](https://github.com/ssi-dk/SCDB).

``` r

ds <- DiseasystoreGoogleCovid19$new(
  target_conn = DBI::dbConnect(duckdb::duckdb()),
  start_date = as.Date("2020-03-01"),
  end_date = as.Date("2020-03-15")
)
#> duckdb keeps downloaded extensions and secrets in a temporary directory:
#> ℹ /tmp/RtmpaJlFSI/duckdb
#> This is removed when the R session ends.
#> • Extensions are re-downloaded each session.
#> • Secrets are lost.
#> ℹ Run duckdb(shared_home = TRUE) (or create ~/.duckdb) to keep them (suitable for most users).
#> ℹ Run duckdb(shared_home = FALSE) to accept the temporary directory (and silence this message).
#> ℹ See ?duckdb_storage for details and alternatives.
```

When we create our new `diseasystore` instance, we also supply
`start_date` and `end_date` arguments. These are not strictly required,
but make getting features for this time interval simpler.

Once configured we can query the available features in the
`diseasystore`

``` r

ds$available_features
#>  [1] "n_population"    "age_group"       "country_id"      "country"        
#>  [5] "region_id"       "region"          "subregion_id"    "subregion"      
#>  [9] "n_hospital"      "n_deaths"        "n_positive"      "n_icu"          
#> [13] "n_ventilator"    "min_temperature" "max_temperature"
```

These features can be retrieved individually (using the `start_date` and
`end_date` we specified during creation of `ds`):

``` r

ds$get_feature("n_hospital")
#> Note: method with signature 'DBIConnection#Id' chosen for function 'dbExistsTable',
#>  target signature 'duckdb_connection#Id'.
#>  "duckdb_connection#ANY" would also be valid
#> # A query:  ?? x 5
#> # Database: DuckDB 2.0.0 [unknown@Linux 7.0.0-1017-azure:R 4.6.2/:memory:]
#>   key_location key_age_bin n_hospital valid_from valid_until
#>   <chr>        <chr>            <dbl> <date>     <date>     
#> 1 AR           0                   NA 2020-03-01 2020-03-02 
#> 2 AR           1                    1 2020-03-01 2020-03-02 
#> 3 AR           2                    1 2020-03-01 2020-03-02 
#> 4 AR           3                   NA 2020-03-01 2020-03-02 
#> 5 AR           4                    1 2020-03-01 2020-03-02 
#> # ℹ more rows
```

Notice that features have associated “key\_\*” and “valid_from/until”
columns. These are used for one of the primary selling points of
`diseasystore`, namely [automatic aggregation](#automatic-aggregation).

Go get features for other time intervals, we can manually supply
`start_date` and/or `end_date`:

``` r

ds$get_feature("n_hospital",
               start_date = as.Date("2020-03-01"),
               end_date = as.Date("2020-03-02"))
#> # A query:  ?? x 5
#> # Database: DuckDB 2.0.0 [unknown@Linux 7.0.0-1017-azure:R 4.6.2/:memory:]
#>   key_location key_age_bin n_hospital valid_from valid_until
#>   <chr>        <chr>            <dbl> <date>     <date>     
#> 1 AR           0                   NA 2020-03-01 2020-03-02 
#> 2 AR           1                    1 2020-03-01 2020-03-02 
#> 3 AR           2                    1 2020-03-01 2020-03-02 
#> 4 AR           3                   NA 2020-03-01 2020-03-02 
#> 5 AR           4                    1 2020-03-01 2020-03-02 
#> # ℹ more rows
```

## Dynamically expanded

The `diseasystore` automatically expands the computed features.

Say a given “n_hospital” has been computed between 2020-03-01 and
2020-03-15. In this case, the call
`$get_feature("n_hospital", start_date = as.Date("2020-03-01"), end_date = as.Date("2020-03-20")`
only needs to compute the feature between 2020-03-16 and 2020-03-20.

## Time versioned

Through using [SCDB](https://github.com/ssi-dk/SCDB) as the back end,
the features are stored even as new data becomes available. This way, we
get a time-versioned record of the features provided by `diseasystore`.

The features being computed is controlled through the `slice_ts`
argument. By default, `diseasystores` uses today’s date for this
argument.

The dynamical expansion of the features described above is only valid
for any given `slice_ts`. That is, if a feature has been computed for a
time interval on one `slice_ts`, `diseasystore` will recompute the
feature for any other `slice_ts`.

This way, feature computation can be implemented into continuous
integration (requesting features will preserve a history of computed
features). Furthermore, post-hoc analysis can be performed by computing
features as they would have looked on previous dates.

## Automatic aggregation

The real strength of `diseasystore` comes from its built-in automatic
aggregation.

We saw above that the features come with additional associated “key\_\*”
and “valid_from/until” columns.

This additional information is used to do automatic aggregation through
the `?DieasystoreBase$key_join_features()` method (see
[extending-diseasystore](https://ssi-dk.github.io/diseasystore/articles/extending-diseasystore.md)
for more details).

To use this method, you need to provide the `observable` that you want
to aggregate and the `stratification` you want to apply to the
aggregation.

To see which features are considered “observables” and which are
considered “stratifications” you can use the included methods:

``` r

ds$available_observables
#> [1] "n_population"    "n_hospital"      "n_deaths"        "n_positive"     
#> [5] "n_icu"           "n_ventilator"    "min_temperature" "max_temperature"
```

``` r

ds$available_stratifications
#> [1] "age_group"    "country_id"   "country"      "region_id"    "region"      
#> [6] "subregion_id" "subregion"
```

Lets start with an simple example where we request no stratification
(`NULL`):

``` r

ds$key_join_features(observable = "n_hospital",
                     stratification = NULL)
#> # A tibble: 15 × 2
#>   date       n_hospital
#>   <date>          <dbl>
#> 1 2020-03-01         NA
#> 2 2020-03-02          6
#> 3 2020-03-03          5
#> 4 2020-03-04         12
#> 5 2020-03-05          8
#> # ℹ 10 more rows
```

This gives us the same feature information as
`ds$get_feature("n_hospital")` but simplified to give the observable per
day (in this case, the number of people hospitalised).

To specify a level of `stratification`, we need to supply a list of
`quosures` (see
[`help("topic-quosure", package = "rlang")`](https://rlang.r-lib.org/reference/topic-quosure.html)).

``` r

ds$key_join_features(observable = "n_hospital",
                     stratification = rlang::quos(country_id))
#> # A tibble: 15 × 3
#>   date       country_id n_hospital
#>   <date>     <chr>           <dbl>
#> 1 2020-03-01 AR                 NA
#> 2 2020-03-02 AR                  6
#> 3 2020-03-03 AR                  5
#> 4 2020-03-04 AR                 12
#> 5 2020-03-05 AR                  8
#> # ℹ 10 more rows
```

The `stratification` argument is very flexible, so we can supply any
valid R expression:

``` r

ds$key_join_features(observable = "n_hospital",
                     stratification = rlang::quos(country_id,
                                                  old = age_group == "90+"))
#> # A tibble: 30 × 4
#>   date       country_id old   n_hospital
#>   <date>     <chr>      <lgl>      <dbl>
#> 1 2020-03-01 AR         FALSE         NA
#> 2 2020-03-02 AR         FALSE         54
#> 3 2020-03-03 AR         FALSE         45
#> 4 2020-03-04 AR         FALSE        108
#> 5 2020-03-05 AR         FALSE         72
#> # ℹ 25 more rows
```

## Dropping computed features

Sometimes, it is need to clear the compute features from the data base.
For this purpose, we provide the
[`drop_diseasystore()`](https://ssi-dk.github.io/diseasystore/reference/drop_diseasystore.md)
function.

By default, this deletes all stored features in the default
`diseasystore` schema. A `pattern` argument to match tables by and a
`schema` argument to specify the schema to delete from[^1].

``` r

SCDB::get_tables(ds$target_conn, show_temporary = FALSE)
#>   catalog schema                        table
#> 1  memory   main ds.google_covid_19_age_group
#> 2  memory   main  ds.google_covid_19_hospital
#> 3  memory   main     ds.google_covid_19_index
#> 4  memory   main                     ds.locks
#> 5  memory   main                      ds.logs
```

``` r

drop_diseasystore(conn = ds$target_conn)

SCDB::get_tables(ds$target_conn, show_temporary = FALSE)
#> [1] catalog schema  table  
#> <0 rows> (or 0-length row.names)
```

## diseasystore options

`diseasystores` have a number of options available to make configuration
easier. These options all start with “diseasystore.”.

To see all options related to `diseasystore`, we can use the
[`diseasyoption()`](https://ssi-dk.github.io/diseasystore/reference/diseasyoption.md)
function without arguments.

``` r

diseasyoption()
#> $diseasystore.DiseasystoreEcdcRespiratoryViruses.pull
#> [1] TRUE
#> 
#> $diseasystore.DiseasystoreEcdcRespiratoryViruses.remote_conn
#> [1] "https://api.github.com/repos/EU-ECDC/Respiratory_viruses_weekly_data"
#> 
#> $diseasystore.DiseasystoreEcdcRespiratoryViruses.source_conn
#> [1] "https://api.github.com/repos/EU-ECDC/Respiratory_viruses_weekly_data"
#> 
#> $diseasystore.DiseasystoreEcdcRespiratoryViruses.target_conn
#> [1] ""
#> 
#> $diseasystore.DiseasystoreEcdcRespiratoryViruses.target_schema
#> [1] ""
#> 
#> $diseasystore.DiseasystoreGoogleCovid19.n_max
#> [1] 1000
#> 
#> $diseasystore.DiseasystoreGoogleCovid19.remote_conn
#> [1] "https://storage.googleapis.com/covid19-open-data/v3/"
#> 
#> $diseasystore.DiseasystoreGoogleCovid19.source_conn
#> [1] "https://storage.googleapis.com/covid19-open-data/v3/"
#> 
#> $diseasystore.DiseasystoreGoogleCovid19.target_conn
#> [1] ""
#> 
#> $diseasystore.DiseasystoreGoogleCovid19.target_schema
#> [1] ""
#> 
#> $diseasystore.lock_wait_increment
#> [1] 15
#> 
#> $diseasystore.lock_wait_max
#> [1] 1800
#> 
#> $diseasystore.source_conn
#> [1] ""
#> 
#> $diseasystore.target_conn
#> [1] ""
#> 
#> $diseasystore.target_schema
#> [1] "ds"
#> 
#> $diseasystore.verbose
#> [1] FALSE
```

This returns all options related to `diseasystore` and its sister
package `{diseasy}`.

If you want the options for a specific `package`, you can use the
`namespace` argument. Notice that several options are set as empty
strings (““). These are treated as `NULL` by `diseasystore`[^2].

Importantly, the options are *scoped*. Consider the above options for
“source_conn”: Looking at the list of options we find
“diseasystore.source_conn” and
“diseasystore.DiseasystoreGoogleCovid19.source_conn”. The former is a
general setting while the latter is specific setting for
[`?DiseasystoreGoogleCovid19`](https://ssi-dk.github.io/diseasystore/reference/DiseasystoreGoogleCovid19.md).
The general setting is used as fallback if no specific setting is found.

This allows you to set a general configuration to use and to overwrite
it for specific cases.

To get the option related to a scope, we can use the
[`diseasyoption()`](https://ssi-dk.github.io/diseasystore/reference/diseasyoption.md)
function.

``` r

diseasyoption("source_conn", class = "DiseasystoreGoogleCovid19")
#> [1] "https://storage.googleapis.com/covid19-open-data/v3/"
```

As we saw in the options, a `source_conn` option was defined
specifically for
[`?DiseasystoreGoogleCovid19`](https://ssi-dk.github.io/diseasystore/reference/DiseasystoreGoogleCovid19.md).

If we try the same for the hypothetical `DiseasystoreDiseaseY`, we see
that no value is defined as we have not yet configured the fallback
value.

``` r

diseasyoption("source_conn", class = "DiseasystoreDiseaseY")
#> NULL
```

If we change our general setting for `source_conn` and retry, we see
that we get the fallback value.

``` r

options("diseasystore.source_conn" = file.path("local", "path"))
diseasyoption("source_conn", class = "DiseasystoreDiseaseY")
#> [1] "local/path"
```

Finally, we can use the `.default` argument as a final fallback value in
case no option is set for either general or specific case.

``` r

diseasyoption(
  "non_existent",
  class = "DiseasystoreDiseaseY",
  .default = "final fallback"
)
#> [1] "final fallback"
```

[^1]: If using `SQLite` as the back end, it will instead prepend the
    schema specification to the pattern before matching
    (e.g. “ds\\.\*“).

[^2]: R’s [`options()`](https://rdrr.io/r/base/options.html) does not
    allow setting an option to `NULL`. By setting options as empty
    strings, the user can see the available options to set.
