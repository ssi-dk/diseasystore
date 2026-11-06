# File path helper for different source_conn

- source_conn_path: static url / directory. This helper determines
  whether source_conn is a file path or URL and creates the full path to
  the the file as needed based on the type of source_conn.

&nbsp;

- source_conn_github: static GitHub API url / git directory. This helper
  determines whether source_conn is a git directory or a GitHub API
  creates the full path to the the file as needed based on the type of
  source_conn.

  A GitHub token can be configured in the "GITHUB_PAT" environment
  variable to avoid rate limiting.

  If the basename of the requested file contains a date, the function
  will use fuzzy-matching to determine the closest matching,
  chronologically earlier, file location to return.

## Usage

``` r
source_conn_path(source_conn, file)

source_conn_github(source_conn, file, pull = TRUE)
```

## Arguments

- source_conn:

  (`character(1)`)  
  \# nolint: documentation_template_linter File location (path or URL).

- file:

  (`character(1)`)  
  Name (including path) of the file at the location.

- pull:

  (`logical(1)`)  
  Should "git pull" be called on the local repository before reading
  files?

## Value

(`character(1)`)  
The full path to the requested file.

## Examples

``` r
  # Simulating a data directory
  source_conn <- "data_dir"
  dir.create(source_conn)
  write.csv(mtcars, file.path(source_conn, "mtcars.csv"))
  write.csv(iris, file.path(source_conn, "iris.csv"))

  # Get file path for mtcars.csv
  source_conn_path(source_conn, "mtcars.csv")
#> [1] "data_dir/mtcars.csv"

  # Clean up
  unlink(source_conn, recursive = TRUE)
```
