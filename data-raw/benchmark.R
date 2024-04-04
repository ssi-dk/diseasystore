withr::local_options("odbc.batch_rows" = 1000)

# Load the connection helper
source("tests/testthat/helper-setup.R")

# Compute the version matrix
versions <- expand.grid(
  diseasystore_version = c("CRAN", "main", "branch"),
  scdb_version = c("CRAN", "main")
)

# Install extra dependencies
pak::pkg_install("jsonlite")
pak::pkg_install("microbenchmark")
library("jsonlite")
library("microbenchmark")


# Determine branch status
branch <- system("git symbolic-ref --short HEAD", intern = TRUE)
sha <- system("git rev-parse HEAD", intern = TRUE)

# Create folder to store the different configurations
dir.create("installations", showWarnings = FALSE)

lib_dir_common <- file.path("installations", "common")
dir.create(lib_dir_common, showWarnings = FALSE)


# Install the packages
if (interactive() || (identical(Sys.getenv("CI"), "true") && identical(Sys.getenv("BACKEND"), ""))) {

  # Determine common packages
  common_scdb_packages <- purrr::map(unique(versions$scdb_version), \(scdb_version) {

    scdb_source <- dplyr::case_when(
      scdb_version == "CRAN" ~ "SCDB",
      scdb_version == "main" ~ "ssi-dk/SCDB"
    )

    pak::lockfile_create(scdb_source, "SCDB.lock", dependencies = TRUE)

    return(jsonlite::fromJSON("SCDB.lock")$packages)
  }) |>
    purrr::reduce(dplyr::intersect)

  common_diseasystore_packages <- purrr::map(unique(versions$diseasystore_version), \(diseasystore_version) {

    if (diseasystore_version == "branch" && branch == "main") {
      return(NULL)
    }

    diseasystore_source <- dplyr::case_when(
      diseasystore_version == "CRAN" ~ "diseasystore",
      diseasystore_version == "main" ~ "ssi-dk/diseasystore",
      diseasystore_version == "branch" ~ glue::glue("ssi-dk/diseasystore@{sha}")
    )

    pak::lockfile_create(diseasystore_source, "diseasystore.lock", dependencies = TRUE)

    return(jsonlite::fromJSON("diseasystore.lock")$packages)
  }) |>
    purrr::reduce(dplyr::intersect)

  # Determine the already installed packages (from setup-r-dependecies workflow)
  pak::lockfile_create(dependencies = TRUE)
  preinstalled_packages <- jsonlite::fromJSON("pkg.lock")$packages


  # Create lockfile with intersection of packages and install
  common_packages <- dplyr::setdiff(dplyr::union(common_scdb_packages, common_diseasystore_packages), preinstalled_packages)
  lockfile <- jsonlite::fromJSON("SCDB.lock")
  lockfile$packages <- common_packages
  writeLines(jsonlite::toJSON(lockfile, pretty = TRUE), "common.lock")
  pak::lockfile_install("common.lock", lib = lib_dir_common)
  .libPaths(lib_dir_common)

  # Store all installed packages at this point
  preinstalled_and_common_packages <- dplyr::union(common_packages, preinstalled_packages)


  # Pre-install the remaining packages
  purrr::pwalk(versions, \(diseasystore_version, scdb_version) {

    if (diseasystore_version == "branch" && branch == "main") {
      return(NULL)
    }

    diseasystore_source <- dplyr::case_when(
      diseasystore_version == "CRAN" ~ "diseasystore",
      diseasystore_version == "main" ~ "ssi-dk/diseasystore",
      diseasystore_version == "branch" ~ glue::glue("ssi-dk/diseasystore@{sha}")
    )


    scdb_source <- dplyr::case_when(
      scdb_version == "CRAN" ~ "SCDB",
      scdb_version == "main" ~ "ssi-dk/SCDB"
    )

    lib_dir <- file.path("installations", glue::glue("{diseasystore_version}_{scdb_version}"))

    dir.create(lib_dir, showWarnings = FALSE)
    .libPaths(c(lib_dir, lib_dir_common))

    pak::lockfile_create(scdb_source, "SCDB.lock", dependencies = TRUE)
    pak::lockfile_create(diseasystore_source, "diseasystore.lock", dependencies = TRUE)

    scdb_lockfile <- jsonlite::fromJSON("SCDB.lock")
    scdb_lockfile$packages <- dplyr::setdiff(scdb_lockfile$packages, preinstalled_and_common_packages)
    writeLines(jsonlite::toJSON(scdb_lockfile, pretty = TRUE), "SCDB.lock")

    diseasystore_lockfile <- jsonlite::fromJSON("diseasystore.lock")
    diseasystore_lockfile$packages <- dplyr::setdiff(diseasystore_lockfile$packages, preinstalled_and_common_packages)
    writeLines(jsonlite::toJSON(diseasystore_lockfile, pretty = TRUE), "diseasystore.lock")

    tryCatch({
      pak::lockfile_install("SCDB.lock", lib = lib_dir)
    }, error = function(e) {
      pak::pkg_install(scdb_source, lib = lib_dir, dependencies = TRUE)
    })

    tryCatch({
      pak::lockfile_install("diseasystore.lock", lib = lib_dir)
    }, error = function(e) {
      pak::pkg_install(diseasystore_source, lib = lib_dir, dependencies = TRUE)
    })
  })

}



# Then loop over each and run the benchmarks
if (interactive() || (identical(Sys.getenv("CI"), "true") && !identical(Sys.getenv("BACKEND"), ""))) {
  purrr::pwalk(versions, \(diseasystore_version, scdb_version) {

    # R does not like to make sense, so we need to ensure that we don't have factors
    diseasystore_version <- as.character(diseasystore_version)
    scdb_version <- as.character(scdb_version)

    # Use the pre-installed packages
    lib_dir <- file.path("installations", glue::glue("{diseasystore_version}_{scdb_version}"))
    .libPaths(c(lib_dir, lib_dir_common))
    library("diseasystore")


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
      n <- 10
      slow_backends <- c("DuckDB", "MSSQL")

      n <- ifelse(any(stringr::str_starts(names(conns)[1], slow_backends)), ceiling(n / 2), n)
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
          "SCDB" = factor(scdb_version, levels = c("CRAN", "main")),
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
          "SCDB" = factor(scdb_version, levels = c("CRAN", "main")),
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
