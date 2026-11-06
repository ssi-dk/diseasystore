# diseasystore: ECDC Respiratory Viruses Weekly

``` r

library(diseasystore)
```

The Respiratory viruses weekly repository contains weekly summaries of
the
[erviss](https://www.ecdc.europa.eu/en/publications-data/european-respiratory-virus-surveillance-summary-erviss)
data for Influenza-Like Illness (ILI) and Acute Respiratory Infection
(ARI).

This vignette shows how to use (some of) this data through the
`diseasystore` package.

First, it is a good idea to clone the repository locally and store that
location as an option for the package.

``` r

# First we set the path we want to use as an option
options(
  "diseasystore.DiseasystoreEcdcRespiratoryViruses.source_conn" =
    file.path("local", "path")
)

# Ensure folder exists
source_conn <- diseasyoption("source_conn", "DiseasystoreEcdcRespiratoryViruses")
if (!dir.exists(source_conn)) {
  dir.create(source_conn, recursive = TRUE, showWarnings = FALSE)
}

# Clone the repository
system2(
  "git",
  args = c(
    paste("-C", source_conn),
    "clone https://github.com/EU-ECDC/Respiratory_viruses_weekly_data"
  ),
  stdout = TRUE
)
```

The `diseasystores` require a database to store its features in. These
should be configured before use and can be stored in the packages
options.

``` r

# We define target_conn as a function that opens a DBIconnection to the DB
target_conn <- \() DBI::dbConnect(duckdb::duckdb())
options(
  "diseasystore.DiseasystoreEcdcRespiratoryViruses.target_conn" = target_conn
)
```

Once the files are downloaded and the target database is configured, we
can initialize the `diseasystore` that uses the ECDC Respiratory Viruses
Weekly data.

``` r

ds <- DiseasystoreEcdcRespiratoryViruses$new()
#> duckdb keeps downloaded extensions and secrets in a temporary directory:
#> ℹ /tmp/RtmpnY5LEI/duckdb
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
#> [1] "iliari_rates"   "infection_type" "age_group"
```

If you have locally cloned the repository, you may consider pulling data
from the repository manually and disabling automatic pulls. Otherwise,
the `diseasystore` will check for updates every time you request a
feature.

``` r

# Manually update the repository
system2(
  "git",
  args = paste(
    "-C",
    diseasyoption("source_conn", "DiseasystoreEcdcRespiratoryViruses")
  ),
  stdout = TRUE
)

# Disable automatic pulls
options("diseasystore.DiseasystoreEcdcRespiratoryViruses.pull" = FALSE)
```

``` r

# And then retrieve a feature from the feature store
ds$get_feature(
  feature = "iliari_rates",
  start_date = as.Date("2023-01-01"),
  end_date = as.Date("2023-03-01")
)
#> Note: method with signature 'DBIConnection#Id' chosen for function 'dbExistsTable',
#>  target signature 'duckdb_connection#Id'.
#>  "duckdb_connection#ANY" would also be valid
#> # A query:  ?? x 6
#> # Database: DuckDB 2.0.0 [unknown@Linux 7.0.0-1017-azure:R 4.6.2/:memory:]
#>   key_location age_group infection_type   rate valid_from valid_until
#>   <chr>        <chr>     <chr>           <dbl> <date>     <date>     
#> 1 Austria      00-04     ILI            10576. 2023-02-27 2023-03-06 
#> 2 Austria      15-64     ILI             3249. 2023-02-27 2023-03-06 
#> 3 Austria      05-14     ILI             4752. 2023-02-27 2023-03-06 
#> 4 Austria      65+       ILI              404. 2023-02-27 2023-03-06 
#> 5 Austria      NA        ILI             3299. 2023-02-27 2023-03-06 
#> # ℹ more rows
```
