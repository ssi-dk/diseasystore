test_that("nolint_linter works", {
  lintr::expect_lint(
    paste0(strrep("x", 5), "# nolint: object_name_linter"),
    list("line_number" = 6, "type" = "style"),
    nolint_linter(length = 10)
  )

  lintr::expect_lint(
    paste0(strrep("x", 10), "# nolint: object_name_linter"),
    NULL,
    nolint_linter(length = 10))
})
