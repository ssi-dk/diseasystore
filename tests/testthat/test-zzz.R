test_that(r"{options are conserved during testing}", {
  current_diseasy_opts <- purrr::keep(names(options()), ~ startsWith(., "diseasy")) |>
    purrr::map(options) |>
    purrr::reduce(c)

  expect_identical(current_diseasy_opts, diseasy_opts)
})
