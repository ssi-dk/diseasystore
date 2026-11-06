# Backend-dependent time interval (in years)

Provides the sql code for a time interval (in years).

## Usage

``` r
add_years(reference_date, years, conn)
```

## Arguments

- reference_date:

  (`Date(1)` or `character(1)`)  
  The date to add years to (or name of column containing the reference
  date).

- years:

  (`numeric(1)` or `character(1)`)  
  The length of the time interval in whole years (or name of column
  containing the number of years).

- conn:

  (`DBIConnection` or `function`)  
  A database connection or function that opens a database connection.

## Value

SQL query for the time interval.

## Examples

``` r
  conn <- SCDB::get_connection(drv = RSQLite::SQLite())

  dplyr::copy_to(conn, data.frame(birth = as.Date("2001-04-03"), "test_age")) |>
    dplyr::mutate(first_birthday = !!add_years("birth", 1, conn))
#> Error in pkgdown::build_site_github_pages(new_process = FALSE, install = TRUE): (converted from warning) Time computation on SQLite is not precise! For long time intervals, the result may be off by 1+ days. Consider using DuckDB as a local database with precise age computation.

  DBI::dbDisconnect(conn)
```
