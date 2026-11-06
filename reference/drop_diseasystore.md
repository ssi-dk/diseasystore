# Drop feature stores from DB

Drop feature stores from DB

## Usage

``` r
drop_diseasystore(
  pattern = NULL,
  schema = diseasyoption("target_schema", namespace = "diseasystore"),
  conn = SCDB::get_connection()
)
```

## Arguments

- pattern:

  (`character(1)`)  
  Pattern to match the tables by

- schema:

  (`character`)  
  A database schema. If the database backend does not support schema,
  the tables will be prefixed with `<schema>.` The location where the
  `diseasystore` stores data.

- conn:

  (`DBIConnection` or `function`)  
  A database connection or function that opens a database connection.

## Value

`NULL` (called for side effects)

## Examples

``` r
  conn <- SCDB::get_connection(drv = RSQLite::SQLite())

  drop_diseasystore(conn = conn)
#> NULL

  DBI::dbDisconnect(conn)
```
