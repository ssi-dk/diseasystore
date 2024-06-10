withr::local_options("odbc.batch_rows" = 1000)

# Install extra dependencies
pak::pkg_install("jsonlite")
pak::pkg_install("microbenchmark")
pak::pkg_install("here")


# Load the connection helper
source("tests/testthat/helper-setup.R")

# Compute the version matrix
versions <- expand.grid(
  diseasystore_version = c("CRAN", "main", "branch"),
  scdb_version = c("CRAN", "main")
)

# Determine branch status
branch <- system("git symbolic-ref --short HEAD", intern = TRUE)
sha <- system("git rev-parse HEAD", intern = TRUE)

# Create folder to store the different configurations
dir.create("installations", showWarnings = FALSE)

lib_dir_common <- here::here("installations", "common")
dir.create(lib_dir_common, showWarnings = FALSE)
lib_paths_common <- c(lib_dir_common, .libPaths()[1])


# Install the remaining packages
if (interactive() || (identical(Sys.getenv("CI"), "true") && identical(Sys.getenv("BACKEND"), ""))) {

  # Determine common packages
  scdb_packages <- purrr::map(unique(versions$scdb_version), \(scdb_version) {

    scdb_source <- dplyr::case_when(
      scdb_version == "CRAN" ~ "SCDB",
      scdb_version == "main" ~ "ssi-dk/SCDB"
    )

    pak::lockfile_create(scdb_source, "SCDB.lock", dependencies = TRUE)

    return(jsonlite::fromJSON("SCDB.lock")$packages)
  })

  lockfile_signature <- scdb_packages |>
    purrr::map(~ head(.x, 0)) |>
    purrr::reduce(dplyr::bind_rows)

  common_scdb_packages <- scdb_packages |>
    purrr::map(~ dplyr::bind_rows(.x, lockfile_signature)) |>
    purrr::reduce(dplyr::intersect)

  diseasystore_packages <- purrr::map(unique(versions$diseasystore_version), \(diseasystore_version) {

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
  })

  lockfile_signature <- diseasystore_packages |>
    purrr::map(~ head(.x, 0)) |>
    purrr::reduce(dplyr::bind_rows)

  common_diseasystore_packages <- diseasystore_packages |>
    purrr::map(~ dplyr::bind_rows(.x, lockfile_signature)) |>
    purrr::reduce(dplyr::intersect)

  # Install the missing packages
  dplyr::union(common_scdb_packages$ref, common_diseasystore_packages$ref) |>
    purrr::discard(rlang::is_installed) |>
    pak::pkg_install(lib = lib_dir_common, dependencies = FALSE)


  # Pre-install the version specific packages
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


    lib_dir <- here::here("installations", glue::glue("{diseasystore_version}_{scdb_version}"))
    dir.create(lib_dir, showWarnings = FALSE)
    .libPaths(c(lib_dir, lib_paths_common))


    # Install dependencies
    pak::lockfile_create(scdb_source, "SCDB.lock", dependencies = TRUE)
    pak::lockfile_create(diseasystore_source, "diseasystore.lock", dependencies = TRUE)

    union(jsonlite::fromJSON("SCDB.lock")$packages$ref, jsonlite::fromJSON("diseasystore.lock")$packages$ref) |>
      purrr::discard(rlang::is_installed) |>
      pak::pkg_install(lib = lib_dir, dependencies = FALSE)

    # Explicitly install the packages
    pak::pkg_install(scdb_source, lib = lib_dir, dependencies = FALSE)
    pak::pkg_install(diseasystore_source, lib = lib_dir, dependencies = FALSE)

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

    library("diseasystore", lib.loc = lib_dir)                                                                          # nolint: library_call_linter

    # Add proper version labels to the benchmarks
    if (scdb_version == "CRAN") {
      scdb_version <- paste0("SCDB v",  packageVersion("SCDB", lib.loc = lib_dir))
    }
    if (diseasystore_version == "CRAN") {
      diseasystore_version <- paste0("diseasystore v",  packageVersion("diseasystore", lib.loc = lib_dir))
    }

    try({
      # Create a dummy DiseasystoreBase with a mtcars FeatureHandler
      diseasystore_generator <- \(benchmark_data) {
        R6::R6Class(
          classname = "DiseasystoreBase",
          inherit = DiseasystoreBase,
          private = list(
            .ds_map = list("n_cyl" = "dummy_cyl", "vs" = "dummy_vs"),
            dummy_cyl = FeatureHandler$new(
              compute = function(start_date, end_date, slice_ts, source_conn, ...) {
                out <- benchmark_data |>
                  dplyr::transmute(
                    "key_car" = .data$car, "n_cyl" = .data$cyl,
                    "valid_from" = Sys.Date() - lubridate::days(2 * .data$row_id - 1),
                    "valid_until" = .data$valid_from + lubridate::days(2)
                  )
                return(out)
              },
              key_join = key_join_sum
            ),
            dummy_vs = FeatureHandler$new(
              compute = function(start_date, end_date, slice_ts, source_conn, ...) {
                out <- benchmark_data |>
                  dplyr::transmute(
                    "key_car" = .data$car, .data$vs,
                    "valid_from" = Sys.Date() - lubridate::days(2 * .data$row_id),
                    "valid_until" = .data$valid_from + lubridate::days(2)
                  )
                return(out)
              },
              key_join = key_join_sum
            )
          )
        )
      }


      # Our benchmark data is the mtcars data set but repeated to increase the data size
      # If updated, please also update the version in the vignettes/benchmark.Rmd
      data_generator <- function(repeats) {
        purrr::map(
          seq(repeats),
          \(i) {
            dplyr::mutate(
              mtcars,
              "row_id" = dplyr::row_number() + (i - 1) * nrow(mtcars),
              "car" = paste(rownames(mtcars), .data$row_id)
            )
          }
        ) |>
          purrr::reduce(rbind) |>
          dplyr::rename_with(~ tolower(gsub(".", "_", .x, fixed = TRUE)))
      }




      # Benchmark get_feature()
      n <- 1000
      conns <- get_test_conns()
      conn <- conns[[1]]

      # Create new instance for the benchmarks
      ds <- diseasystore_generator(data_generator(n))
      ds <- ds$new(
        target_conn = conn,
        start_date = Sys.Date() - 2 * n * SCDB::nrow(mtcars) - 1,
        end_date = Sys.Date(),
        verbose = FALSE
      )

      # Define the benchmark function
      diseasytore_get_feature <- function(ds) {
        ds$get_feature("n_cyl")
        ds$get_feature("vs")
        drop_diseasystore(schema = ds %.% target_schema, conn = ds %.% target_conn)
      }

      # Run single iteration to ensure no burn in issues
      diseasytore_get_feature(ds)

      # Run the benchmark
      get_feature_benchmark <- microbenchmark::microbenchmark(diseasytore_get_feature(ds), times = 10) |>
        dplyr::mutate(
          "benchmark_function" = "get_feature()",
          "database" = names(conns)[[1]],
          "version" = !!ifelse(diseasystore_version == "branch", substr(sha, 1, 10), diseasystore_version),
          "SCDB" = factor(scdb_version, levels = unique(c(scdb_version, "main"))),
          "n" = n
        )

      dir.create("inst/extdata", showWarnings = FALSE)
      saveRDS(
        get_feature_benchmark,
        glue::glue("inst/extdata/benchmark-get_feature_{names(conns)[[1]]}_{diseasystore_version}_{scdb_version}.rds")
      )

      # Clean up
      purrr::walk(conns, ~ DBI::dbDisconnect(., shutdown = TRUE))



      # Benchmark key_join_features()
      n <- 100
      conns <- get_test_conns()
      conn <- conns[[1]]

      # Create new instance for the benchmarks
      ds <- diseasystore_generator(data_generator(n))
      ds <- ds$new(
        target_conn = conn,
        start_date = Sys.Date() - 2 * n * SCDB::nrow(mtcars) - 1,
        end_date = Sys.Date(),
        verbose = FALSE
      )

      # Pre-compute the features
      ds$get_feature("n_cyl")
      ds$get_feature("vs")

      # Define the benchmark function
      diseasytore_key_join_features <- function(ds) {
        ds$key_join_features("n_cyl", rlang::quos(vs))
      }

      # Run the benchmark
      key_join_benchmark <- microbenchmark::microbenchmark(diseasytore_key_join_features(ds), times = 10) |>
        dplyr::mutate(
          "benchmark_function" = "key_join_features()",
          "database" = names(conns)[[1]],
          "version" = !!ifelse(diseasystore_version == "branch", substr(sha, 1, 10), diseasystore_version),
          "SCDB" = factor(scdb_version, levels = unique(c(scdb_version, "main"))),
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
