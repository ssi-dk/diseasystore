# Compute the age (in years) on a given date

Provides the sql code to compute the age of a person on a given date.

## Usage

``` r
age_on_date(birth, reference_date, conn)
```

## Arguments

- birth:

  (`character(1)`)  
  Name of the birth date column.

- reference_date:

  (`Date(1)` or `character(1)`)  
  The date to compute the age for (or name of column containing the
  reference date).

- conn:

  (`DBIConnection` or `function`)  
  A database connection or function that opens a database connection.

## Value

SQL query that computes the age on the given date.

## Examples

``` r
  conn <- SCDB::get_connection(drv = RSQLite::SQLite())

  dplyr::copy_to(conn, data.frame(birth = as.Date("2001-04-03"), "test_age")) |>
    dplyr::mutate(age = !!age_on_date("birth", as.Date("2024-02-28"), conn))
#> Error in pkgdown::build_site_github_pages(new_process = FALSE, install = TRUE): (converted from warning) Age computation on SQLite is not precise. For some edge-cases, age will be off by 1 year! Consider using DuckDB as a local database with precise age computation.

  DBI::dbDisconnect(conn)
```
