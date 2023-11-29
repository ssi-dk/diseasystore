test_that(r"{options are conserved during testing}", {
  expect_identical(options(purrr::keep(names(options()), ~ startsWith(., "diseasy"))), diseasy_opts)
})
