root <- devtools::package_file()
if (!file.exists(file.path(root, "DESCRIPTION")) ||
      !file.exists(file.path(root, "tests", "testthat", "helper-setup.R"))) {
  stop("Run this script from the package root.", call. = FALSE)
}

pak::pkg_install(c("callr", "microbenchmark", "jsonlite"), ask = FALSE)

branch <- system("git symbolic-ref --short HEAD", intern = TRUE)
git_sha <- system("git rev-parse HEAD", intern = TRUE)

# Store the default lib dir
lib_paths_default <- .libPaths()

# Compute the version matrix
versions <- tidyr::expand_grid(
  diseasystore_version = c("CRAN", "main", switch(branch != "main", "branch")),
  scdb_version = c("CRAN", "main")
)

purrr::pwalk(
  versions,
  \(diseasystore_version, scdb_version) {

    scdb_source <- dplyr::case_when(
      scdb_version == "CRAN" ~ "SCDB",
      scdb_version == "main" ~ "ssi-dk/SCDB"
    )

    diseasystore_source <- dplyr::case_when(
      diseasystore_version == "CRAN" ~ "diseasystore",
      diseasystore_version == "main" ~ "ssi-dk/diseasystore",
      diseasystore_version == "branch" ~ glue::glue("ssi-dk/diseasystore@{git_sha}")
    )

    lib_path <- here::here("installations", glue::glue("{diseasystore_version}_{scdb_version}"))
    dir.create(lib_path, showWarnings = FALSE)

    # Install SCDB and its required dependencies
    message("Installing ", scdb_version, ": ", scdb_source)
    pak::pkg_install(scdb_source, lib = lib_path, dependencies = NA, ask = FALSE)

    # Install diseasystore and its required dependencies
    message("Installing ", diseasystore_version, ": ", diseasystore_source)
    pak::pkg_install(diseasystore_source, lib = lib_path, dependencies = NA, ask = FALSE)
  }
)


# Return early on CI if there is no configured backend.
if (identical(Sys.getenv("CI"), "true") && !nzchar(Sys.getenv("BACKEND"))) {
  message("No BACKEND configured in CI; skipping benchmarks.")
} else {

  # Then loop over each and run the benchmarks
  if (interactive() || (identical(Sys.getenv("CI"), "true") && !identical(Sys.getenv("BACKEND"), ""))) {
    purrr::pwalk(
      versions,
      \(diseasystore_version, scdb_version) {

        # Use the pre-installed packages
        lib_path <- here::here("installations", glue::glue("{diseasystore_version}_{scdb_version}"))

        callr::r(
          args = list(
            diseasystore_version = diseasystore_version,
            scdb_version = scdb_version,
            root = root,
            lib_path = lib_path,
            git_sha = git_sha
          ),
          libpath = c(lib_path, lib_paths_default),
          user_profile = FALSE,
          system_profile = FALSE,
          show = TRUE,
          func = function(diseasystore_version, scdb_version, root, lib_path, git_sha) {

            # Each call runs in a fresh R process
            options(odbc.batch_rows = 1000)
            loadNamespace("diseasystore", lib.loc = lib_path)

            loaded_path <- normalizePath(getNamespaceInfo("diseasystore", "path"), winslash = "/")
            expected_path <- normalizePath(file.path(lib_path, "diseasystore"), winslash = "/")
            if (!identical(loaded_path, expected_path)) {
              stop("Loaded diseasystore from the wrong library: ", loaded_path, call. = FALSE)
            }

            # Open connection to the database
            source(file.path(root, "tests", "testthat", "helper-setup.R"), local = TRUE)
            conns <- suppressMessages(get_test_conns())
            conn <- conns[[1]]
            backend <- names(conns)[[1]]

            # Create a dummy DiseasystoreBase with a mtcars FeatureHandler
            diseasystore_generator <- \(benchmark_data) {
              R6::R6Class(                                                                                              # nolint: namespace_linter
                classname = "DiseasystoreBase",
                inherit = diseasystore::DiseasystoreBase,
                private = list(
                  .ds_map = list("n_cyl" = "dummy_cyl", "vs" = "dummy_vs"),
                  dummy_cyl = diseasystore::FeatureHandler$new(
                    compute = function(start_date, end_date, slice_ts, source_conn, ...) {
                      out <- benchmark_data |>
                        dplyr::transmute(
                          "key_car" = .data$car, "n_cyl" = .data$cyl,
                          "valid_from" = Sys.Date() - lubridate::days(2 * .data$row_id - 1),
                          "valid_until" = .data$valid_from + lubridate::days(2)
                        )
                      return(out)
                    },
                    key_join = diseasystore::key_join_sum
                  ),
                  dummy_vs = diseasystore::FeatureHandler$new(
                    compute = function(start_date, end_date, slice_ts, source_conn, ...) {
                      out <- benchmark_data |>
                        dplyr::transmute(
                          "key_car" = .data$car, .data$vs,
                          "valid_from" = Sys.Date() - lubridate::days(2 * .data$row_id),
                          "valid_until" = .data$valid_from + lubridate::days(2)
                        )
                      return(out)
                    },
                    key_join = diseasystore::key_join_sum
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

            scdb_package_version <- as.character(utils::packageVersion("SCDB", lib.loc = lib_path))

            scdb_label <- switch(
              scdb_version,
              CRAN = paste0("SCDB v", scdb_package_version),
              main = "main"
            )


            diseasystore_package_version <- as.character(utils::packageVersion("diseasystore", lib.loc = lib_path))

            diseasystore_label <- switch(
              diseasystore_version,
              CRAN = paste0("diseasystore v", diseasystore_package_version),
              main = "main",
              branch = git_sha
            )

            # Keep the original fields consumed by the benchmark vignette.
            add_metadata <- function(result, benchmark_function, n) {
              result$benchmark_function <- benchmark_function
              result$n <- n
              result$database <- backend
              result$version <- diseasystore_label
              result$SCDB <- factor(scdb_version, levels = unique(c(scdb_version, "main")))
              result$scdb_package_version <- scdb_package_version
              result$diseasystore_package_version <- diseasystore_package_version
              result$r_version <- as.character(getRversion())
              result$run_at <- Sys.time()
              return(result)
            }

            # Clean up
            purrr::walk(conns, ~ DBI::dbDisconnect(., shutdown = TRUE))

            message("Benchmarking ", diseasystore_label, " (", scdb_label, ") on ", backend)

            # Benchmark 1: get_feature()
            n <- 1000
            conns <- suppressMessages(get_test_conns())
            conn <- conns[[1]]

            # Create new instance for the benchmarks
            ds <- diseasystore_generator(data_generator(n))
            ds <- ds$new(
              target_conn = conn,
              start_date = Sys.Date() - 2 * n * SCDB::nrow(mtcars) - 1,
              end_date = Sys.Date(),
              verbose = FALSE
            )

            # Pre-compute the features once to ensure no burn-in issues (e.g. log table creation)
            ds$get_feature("n_cyl")
            ds$get_feature("vs")

            # Ensure we start from a clean state
            diseasystore::drop_diseasystore(schema = ds$target_schema, conn = ds$target_conn)

            # Define the benchmark function
            diseasytore_get_feature <- function(ds) {
              ds$get_feature("n_cyl")
              ds$get_feature("vs")
            }

            # Run the benchmark
            get_feature_benchmark <- microbenchmark::microbenchmark(
              diseasytore_get_feature(ds),
              times = 10,
              setup = diseasystore::drop_diseasystore(schema = ds$target_schema, conn = ds$target_conn)
            ) |>
              add_metadata(benchmark_function = "get_feature()", n = n)
            diseasystore::drop_diseasystore(schema = ds$target_schema, conn = ds$target_conn)


            output_dir <- file.path(root, "inst", "extdata")
            dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

            saveRDS(
              get_feature_benchmark,
              file.path(
                output_dir,
                glue::glue("benchmark-get_feature_{backend}_{diseasystore_version}_{scdb_version}.rds")
              )
            )

            # Clean up
            purrr::walk(conns, ~ DBI::dbDisconnect(., shutdown = TRUE))

            message("  Completed benchmark 1")


            # Benchmark 2: key_join_features
            n <- 100
            conns <- suppressMessages(get_test_conns())
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
            key_join_benchmark <- microbenchmark::microbenchmark(
              diseasytore_key_join_features(ds),
              times = 10
            ) |>
              add_metadata("key_join_features()", n = n)

            saveRDS(
              key_join_benchmark,
              file.path(
                output_dir,
                glue::glue("benchmark-key_join_features_{backend}_{diseasystore_version}_{scdb_version}.rds")
              )
            )


            # Clean up
            purrr::walk(conns, ~ DBI::dbDisconnect(., shutdown = TRUE))

            message("  Completed benchmark 2")
          }
        )
      }
    )
  }
}
