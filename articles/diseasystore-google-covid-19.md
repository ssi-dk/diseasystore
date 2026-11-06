# diseasystore: Google Health COVID-19 Open Data

``` r

library(diseasystore)
```

The Google COVID-19 data repository is a comprehensive open repository
of COVID-19 data.

This vignette shows how to use (some of) this data through the
`diseasystore` package.

First, it is a good idea to copy the relevant Google COVID-19 data files
locally and store that location as an option for the package.
[`?DiseasystoreGoogleCovid19`](https://ssi-dk.github.io/diseasystore/reference/DiseasystoreGoogleCovid19.md)
uses only the age-stratified metrics for COVID-19, so only a subset of
the repository is needed to download.

``` r

# First we set the path we want to use as an option
options(
  "diseasystore.DiseasystoreGoogleCovid19.source_conn" =
    file.path("local", "path")
)

# Ensure folder exists
source_conn <- diseasyoption("source_conn", "DiseasystoreGoogleCovid19")
if (!dir.exists(source_conn)) {
  dir.create(source_conn, recursive = TRUE, showWarnings = FALSE)
}

# Define the Google files to download
google_files <- c("by-age.csv", "demographics.csv", "index.csv", "weather.csv")

# Download each file and compress them to reduce storage
purrr::walk(google_files, ~ {
  url <- paste0(diseasyoption("remote_conn", "DiseasystoreGoogleCovid19"), .)

  destfile <- file.path(
    diseasyoption("source_conn", "DiseasystoreGoogleCovid19"),
    .
  )

  if (!file.exists(destfile)) {
    download.file(url, destfile, mode = "wb")
  }
})
```

The `diseasystores` require a database to store its features in. These
should be configured before use and can be stored in the packages
options.

``` r

# We define target_conn as a function that opens a DBIconnection to the DB
target_conn <- \() DBI::dbConnect(duckdb::duckdb())
options(
  "diseasystore.DiseasystoreGoogleCovid19.target_conn" = target_conn
)
```

Once the files are downloaded and the target DB is configured, we can
initialize the `diseasystore` that uses the Google COVID-19 data.

``` r

ds <- DiseasystoreGoogleCovid19$new()
#> duckdb keeps downloaded extensions and secrets in a temporary directory:
#> ℹ /tmp/RtmpmdUXdp/duckdb
#> This is removed when the R session ends.
#> • Extensions are re-downloaded each session.
#> • Secrets are lost.
#> ℹ Run duckdb(shared_home = TRUE) (or create ~/.duckdb) to keep them (suitable for most users).
#> ℹ Run duckdb(shared_home = FALSE) to accept the temporary directory (and silence this message).
#> ℹ See ?duckdb_storage for details and alternatives.
```

Once configured such, we can use the feature store directly to get data.

``` r

# We can see all the available features in the feature store
ds$available_features
#>  [1] "n_population"    "age_group"       "country_id"      "country"        
#>  [5] "region_id"       "region"          "subregion_id"    "subregion"      
#>  [9] "n_hospital"      "n_deaths"        "n_positive"      "n_icu"          
#> [13] "n_ventilator"    "min_temperature" "max_temperature"
```

``` r

# And then retrieve a feature from the feature store
ds$get_feature(feature = "n_hospital",
               start_date = as.Date("2020-01-01"),
               end_date = as.Date("2020-06-01"))
#> Note: method with signature 'DBIConnection#Id' chosen for function 'dbExistsTable',
#>  target signature 'duckdb_connection#Id'.
#>  "duckdb_connection#ANY" would also be valid
#> # A query:  ?? x 5
#> # Database: DuckDB 2.0.0 [unknown@Linux 7.0.0-1017-azure:R 4.6.2/:memory:]
#>   key_location key_age_bin n_hospital valid_from valid_until
#>   <chr>        <chr>            <dbl> <date>     <date>     
#> 1 AR           0                   NA 2020-01-01 2020-01-02 
#> 2 AR           1                   NA 2020-01-01 2020-01-02 
#> 3 AR           2                    0 2020-01-01 2020-01-02 
#> 4 AR           3                    0 2020-01-01 2020-01-02 
#> 5 AR           4                   NA 2020-01-01 2020-01-02 
#> # ℹ more rows
```
