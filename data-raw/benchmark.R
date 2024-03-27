pak::cache_clean()
withr::local_options("odbc.batch_rows" = 1000)

# Load the connection helper
source("tests/testthat/helper-setup.R")

# Compute the version matrix
versions <- expand.grid(
  diseasystore_version = c("CRAN", "main", "branch"),
  scdb_version = c("CRAN", "main")
)

# Install all needed package versions
purrr::pwalk(versions, \(diseasystore_version, scdb_version) {

  branch <- system("git symbolic-ref --short HEAD", intern = TRUE)
  if (diseasystore_version == "branch" && branch == "main") {
    return(NULL)
  }


  diseasystore_source <- dplyr::case_when(
    diseasystore_version == "CRAN" ~ "diseasystore",
    diseasystore_version == "main" ~ "ssi-dk/diseasystore",
    diseasystore_version == "branch" ~ glue::glue("ssi-dk/diseasystore@{branch}")
  )

  scdb_source <- dplyr::case_when(
    scdb_version == "CRAN" ~ "SCDB",
    scdb_version == "main" ~ "ssi-dk/SCDB"
  )

  pak::pkg_install(
    diseasystore_source,
    lib = glue::glue("installations/diseasystore_{diseasystore_version}"),
    dependencies = TRUE
  )
  pak::pkg_install(
    c(scdb_source, "microbenchmark"),
    lib = glue::glue("installations/SCDB_{scdb_version}"),
    dependencies = TRUE
  )
})


# Return early if no backend is defined
if (identical(Sys.getenv("BACKEND"), "")) {
  message("No backend defined, skipping benchmark!")
  return(NULL)
}


# Then loop over each and benchmark the update_snapshot function
purrr::pwalk(versions, \(diseasystore_version, scdb_version) {

  branch <- system("git symbolic-ref --short HEAD", intern = TRUE)
  if (diseasystore_version == "branch" && branch == "main") {
    return(NULL)
  }

  diseasystore_source <- dplyr::case_when(
    diseasystore_version == "CRAN" ~ "diseasystore",
    diseasystore_version == "main" ~ "ssi-dk/diseasystore",
    diseasystore_version == "branch" ~ glue::glue("ssi-dk/diseasystore@{branch}")
  )

  scdb_source <- dplyr::case_when(
    scdb_version == "CRAN" ~ "SCDB",
    scdb_version == "main" ~ "ssi-dk/SCDB"
  )


  # Load the packages / set the package paths
  library("diseasystore", lib.loc = glue::glue("installations/diseasystore_{diseasystore_version}"))
  .libPaths(glue::glue("installations/SCDB_{scdb_version}"))

  try({
    # Our benchmark data is the mtcars data set but repeated to increase the data size
    data_generator <- function(repeats) {
      purrr::map(
        seq(repeats),
        \(i) dplyr::mutate(mtcars, r = dplyr::row_number() + (i - 1) * nrow(mtcars), "car" = paste(rownames(mtcars), r))
      ) |>
        purrr::reduce(rbind) |>
        dplyr::rename_with(~ tolower(gsub(".", "_", .x, fixed = TRUE)))
    }

    conns <- get_test_conns()
    conn <- conns[[1]]

    # Copy data to the conns
    n <- 4
    slow_backends <- c("DuckDB", "MSSQL")

    n <- ifelse(names(conns)[1] %in% slow_backends, ceiling(n / 2), n)
    benchmark_data <- data_generator(n)

    # Create a dummy DiseasystoreBase with a mtcars FeatureHandler
    DiseasystoreDummy <- R6::R6Class(                                                                                   # nolint: object_name_linter
      classname = "DiseasystoreBase",
      inherit = DiseasystoreBase,
      private = list(
        .ds_map = list("n_cyl" = "dummy_cyl", "vs" = "dummy_vs"),
        dummy_cyl = FeatureHandler$new(
          compute = function(start_date, end_date, slice_ts, source_conn) {
            out <- benchmark_data |>
              dplyr::transmute(
                "key_car" = .data$car, "n_cyl" = .data$cyl,
                "valid_from" = Sys.Date() - lubridate::days(2 * .data$r - 1),
                "valid_until" = .data$valid_from + lubridate::days(2)
              )
            return(out)
          },
          key_join = key_join_sum
        ),
        dummy_vs = FeatureHandler$new(
          compute = function(start_date, end_date, slice_ts, source_conn) {
            out <- benchmark_data |>
              dplyr::transmute(
                "key_car" = .data$car, .data$vs,
                "valid_from" = Sys.Date() - lubridate::days(2 * .data$r),
                "valid_until" = .data$valid_from + lubridate::days(2)
              )
            return(out)
          },
          key_join = key_join_sum
        )
      )
    )

    # Create new instance for the benchmarks
    start_date <- Sys.Date() - 2 * SCDB::nrow(benchmark_data) - 1
    end_date <- Sys.Date()
    ds <- DiseasystoreDummy$new(target_conn = conn, start_date = start_date, end_date = end_date, verbose = FALSE)

    # Define the benchmark functions
    diseasytore_get_feature <- function(ds) {
      ds$get_feature("n_cyl")
      ds$get_feature("vs")
      drop_diseasystore()
    }

    diseasytore_key_join_features <- function(ds) {
      ds$key_join_features("n_cyl", "vs")
    }

    # Construct the list of benchmarks

    ## get_feature
    get_feature_benchmark <- microbenchmark::microbenchmark(diseasytore_get_feature(ds), times = 10) |>
      dplyr::mutate(
        "benchmark_function" = "get_feature()",
        "database" = names(conns)[[1]],
        "version" = !!ifelse(diseasystore_version == "branch", branch, diseasystore_version)
      )

    dir.create("data", showWarnings = FALSE)
    saveRDS(
      get_feature_benchmark,
      glue::glue("data/benchmark-get_feature_{names(conns)[[1]]}_{diseasystore_version}.rds")
    )

    ## key_join_features
    ds$get_feature("n_cyl") # Pre-compute the features
    ds$get_feature("vs")

    key_join_benchmark <- microbenchmark::microbenchmark(diseasytore_key_join_features(ds), times = 10) |>
      dplyr::mutate(
        "benchmark_function" = "key_join_features()",
        "database" = names(conns)[[1]],
        "version" = !!ifelse(diseasystore_version == "branch", branch, diseasystore_version),
        "n" = n
      )

    dir.create("data", showWarnings = FALSE)
    saveRDS(
      key_join_benchmark,
      glue::glue("data/benchmark-key_join_features_{names(conns)[[1]]}_{diseasystore_version}.rds")
    )

    # Clean up
    purrr::walk(conns, ~ DBI::dbDisconnect(., shutdown = TRUE))
  })

  detach("package:diseasystore", unload = TRUE)
})
