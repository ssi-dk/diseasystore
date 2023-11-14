test_that("Code contains no non-ASCII characters", {

  # Look for the source of .Rd files
  help_dir <- system.file("help", package = testthat::testing_package())
  man_dir <- system.file("man", package = testthat::testing_package())

  # Get path to R folder
  r_dir <- system.file("R", package = testthat::testing_package())

  if (checkmate::test_directory_exists(help_dir)) {

    rdx_file <- purrr::keep(dir(help_dir, full.names = TRUE), ~ stringr::str_detect(., ".rdx$"))
    rd_envir <- new.env()
    lazyLoad(stringr::str_remove(rdx_file, ".rdx$"), envir = rd_envir)
    rd_names <- ls(rd_envir)
    rd_files <- purrr::map(rd_names, ~ as.character(eval(purrr::pluck(rd_envir, .))))
    names(rd_files) <- paste0(rd_names, ".Rd")

  } else if (checkmate::test_directory_exists(man_dir)) {

    rd_paths <- purrr::keep(dir(man_dir, full.names = TRUE), ~ stringr::str_detect(., ".[Rr][Dd]$"))
    rd_files <- purrr::map(rd_paths, readLines)
    names(rd_files) <- purrr::map_chr(rd_paths, basename)

  } else {

    stop(".Rd files could not be located")

  }

  # Skip the "*-package.Rd" file
  rd_files <- rd_files[!stringr::str_detect(names(rd_files), "-package.[Rr][Dd]$")]

  if (checkmate::test_directory_exists(r_dir)) {

    r_paths <- purrr::keep(dir(r_dir, full.names = TRUE), ~ stringr::str_detect(., ".[Rr]$"))
    r_files <- purrr::map(r_paths, readLines)
    names(r_paths) <- purrr::map_chr(r_paths, basename)

  } else {

    stop(".R files could not be located")

  }

  files_to_check <- c(r_files, rd_files)

  # Check the files
  for (file_id in seq_along(files_to_check)) {
    has_non_ascii <- purrr::some(files_to_check[[file_id]], ~ stringr::str_detect(., r"{[^\x00-\x7f]}"))
    expect_false(has_non_ascii, label = paste("File:", names(files_to_check)[[file_id]]))
  }
})
