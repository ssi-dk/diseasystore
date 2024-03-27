pak::cache_clean()
withr::local_options("odbc.batch_rows" = 1000)

# Load the connection helper
source("tests/testthat/helper-setup.R")

# Compute the version matrix
versions <- expand.grid(
  diseasystore_version = c("CRAN", "main", "branch"),
  scdb_version = c("CRAN", "main", "branch")
)

# Install all needed package versions
versions |>
  dplyr::pull("diseasystore_version") |>
  levels() |>
  as.list() |>
  purrr::walk(\(version) {

    branch <- system("git symbolic-ref --short HEAD", intern = TRUE)
    sha <- system("git rev-parse HEAD", intern = TRUE)
    if (version == "branch" && branch == "main") {
      return(NULL)
    }


    source <- dplyr::case_when(
      version == "CRAN" ~ "diseasystore",
      version == "main" ~ "ssi-dk/diseasystore",
      version == "branch" ~ glue::glue("ssi-dk/diseasystore@{sha}")
    )

    lib_path <- dplyr::case_when(
      version == "CRAN" ~ "diseasystore",
      version == "main" ~ "ssi-dk-diseasystore",
      version == "branch" ~ glue::glue("ssi-dk-diseasystore-{sha}")
    )

    pak::pkg_install(
      c(source, "jsonlite"),
      lib = lib_path,
      dependencies = TRUE
    )
  })


# Install all needed package versions
versions |>
  dplyr::pull("scdb_version") |>
  levels() |>
  as.list() |>
  purrr::walk(\(version) {

    source <- dplyr::case_when(
      version == "CRAN" ~ "SCDB",
      version == "main" ~ "ssi-dk/SCDB",
      version == "branch" ~ "ssi-dk/SCDB@feature/simplify_update_snapshot"
    )

    lib_path <- dplyr::case_when(
      version == "CRAN" ~ "scdb",
      version == "main" ~ "ssi-dk-scdb",
      version == "branch" ~ "ssi-dk-scdb-branch"
    )

    pak::pkg_install(
      c(source, "microbenchmark", "jsonlite"),
      lib = lib_path,
      dependencies = TRUE
    )
  })



# Return early if no backend is defined
if (identical(Sys.getenv("CI"), "true") && identical(Sys.getenv("BACKEND"), "")) {
  message("No backend defined, skipping benchmark!")
} else {

  # Then loop over each and benchmark the update_snapshot function
  purrr::pwalk(versions, \(diseasystore_version, scdb_version) {

    branch <- system("git symbolic-ref --short HEAD", intern = TRUE)
    sha <- system("git rev-parse HEAD", intern = TRUE)
    if (diseasystore_version == "branch" && branch == "main") {
      return(NULL)
    }

    print("###################")
    print("diseasystore_version")
    print(diseasystore_version)

    print("scdb_version")
    print(scdb_version)

    diseasystore_source <- dplyr::case_when(
      diseasystore_version == "CRAN" ~ "diseasystore",
      diseasystore_version == "main" ~ "ssi-dk/diseasystore",
      diseasystore_version == "branch" ~ glue::glue("ssi-dk/diseasystore@{sha}")
    )

    diseasystore_lib_path <- dplyr::case_when(
      diseasystore_version == "CRAN" ~ "diseasystore",
      diseasystore_version == "main" ~ "ssi-dk-diseasystore",
      diseasystore_version == "branch" ~ glue::glue("ssi-dk-diseasystore-{sha}")
    )


    scdb_source <- dplyr::case_when(
      scdb_version == "CRAN" ~ "SCDB",
      scdb_version == "main" ~ "ssi-dk/SCDB",
      scdb_version == "branch" ~ "ssi-dk/SCDB@feature/simplify_update_snapshot"
    )

    scdb_lib_path <- dplyr::case_when(
      scdb_version == "CRAN" ~ "SCDB",
      scdb_version == "main" ~ "ssi-dk-SCDB",
      scdb_version == "branch" ~ "ssi-dk-SCDB-branch"
    )


    # Load the packages / set the package paths
    library("diseasystore", lib.loc = diseasystore_lib_path)
    .libPaths(scdb_lib_path)


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
          "version" = !!ifelse(diseasystore_version == "branch", substr(sha, 1, 10), diseasystore_version),
          "SCDB" = scdb_version,
          "n" = n
        )

      dir.create("inst/extdata", showWarnings = FALSE)
      saveRDS(
        get_feature_benchmark,
        glue::glue("inst/extdata/benchmark-get_feature_{names(conns)[[1]]}_{diseasystore_version}_{scdb_version}.rds")
      )

      ## key_join_features
      ds$get_feature("n_cyl") # Pre-compute the features
      ds$get_feature("vs")

      key_join_benchmark <- microbenchmark::microbenchmark(diseasytore_key_join_features(ds), times = 10) |>
        dplyr::mutate(
          "benchmark_function" = "key_join_features()",
          "database" = names(conns)[[1]],
          "version" = !!ifelse(diseasystore_version == "branch", substr(sha, 1, 10), diseasystore_version),
          "SCDB" = scdb_version,
          "n" = n
        )

      dir.create("inst/extdata", showWarnings = FALSE)
      saveRDS(
        key_join_benchmark,
        glue::glue(
          "inst/extdata/benchmark-key_join_features_{names(conns)[[1]]}_{diseasystore_version}_{scdb_version}.rds"
        )
      )

      # Clean up
      purrr::walk(conns, ~ DBI::dbDisconnect(., shutdown = TRUE))
    })

    detach("package:diseasystore", unload = TRUE)
  })
}
